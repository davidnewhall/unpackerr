package unpackerr

/* Folder Watching Codez */

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/radovskyb/watcher"
	"golift.io/cnfg"
	"golift.io/xtractr"
)

// defaultPollInterval is used if Docker is detected.
const defaultPollInterval = time.Second

// FolderConfig defines the input data for a watched folder.
type FolderConfig struct {
	DeleteOrig  bool          `json:"delete_original" toml:"delete_original" xml:"delete_original" yaml:"delete_original"`
	DeleteFiles bool          `json:"delete_files" toml:"delete_files" xml:"delete_files" yaml:"delete_files"`
	MoveBack    bool          `json:"move_back" toml:"move_back" xml:"move_back" yaml:"move_back"`
	DeleteAfter cnfg.Duration `json:"delete_after" toml:"delete_after" xml:"delete_after" yaml:"delete_after"`
	ExtractPath string        `json:"extract_path" toml:"extract_path" xml:"extract_path" yaml:"extract_path"`
	Path        string        `json:"path" toml:"path" xml:"path" yaml:"path"`
}

// Folders holds all known (created) folders in all watch paths.
type Folders struct {
	Interval time.Duration
	Config   []*FolderConfig
	Folders  map[string]*Folder
	Events   chan *eventData
	Updates  chan *xtractr.Response
	Printf   func(msg string, v ...interface{})
	Debugf   func(msg string, v ...interface{})
	FSNotify *fsnotify.Watcher
	Watcher  *watcher.Watcher
}

// Folder is a "new" watched folder.
type Folder struct {
	last time.Time
	step ExtractStatus
	cnfg *FolderConfig
	list []string
	retr uint
	rars []string
}

type eventData struct {
	cnfg *FolderConfig
	name string
	file string
}

func (u *Unpackerr) logFolders() {
	if epath, c := "", len(u.Folders); c == 1 {
		if u.Folders[0].ExtractPath != "" {
			epath = ", extract to: " + u.Folders[0].ExtractPath
		}

		u.Printf(" => Folder Config: 1 path: %s%s (delete after:%v, delete orig:%v, move back:%v, event buffer:%d)",
			u.Folders[0].Path, epath, u.Folders[0].DeleteAfter, u.Folders[0].DeleteOrig, u.Folders[0].MoveBack, u.Buffer)
	} else {
		u.Print(" => Folder Config:", c, "paths,", "event buffer:", u.Buffer)

		for _, f := range u.Folders {
			if epath = ""; f.ExtractPath != "" {
				epath = ", extract to: " + f.ExtractPath
			}
			u.Printf(" =>    Path: %s%s (delete after:%v, delete orig:%v, move back:%v)",
				f.Path, epath, f.DeleteAfter, f.DeleteOrig, f.MoveBack)
		}
	}
}

// PollFolders begins the routines to watch folders for changes.
// if those changes include the addition of compressed files, they
// are processed for exctraction.
func (u *Unpackerr) PollFolders() {
	var (
		flist []string
		err   error
	)

	if isRunningInDocker() && u.Folder.Interval.Duration == 0 {
		u.Folder.Interval.Duration = defaultPollInterval
	}

	u.Folders, flist = u.checkFolders()

	if u.folders, err = u.newFolderWatcher(); err != nil {
		u.Print("[ERROR] Watching Folders:", err)
		return
	}
	// do not close either watcher.

	if len(u.Folders) == 0 {
		return
	}

	go u.folders.watchFSNotify()
	u.Print("[Folder] Watching (fsnotify):", strings.Join(flist, ", "))

	if u.Folder.Interval.Duration == 0 {
		return
	}

	go func() {
		if err := u.folders.Watcher.Start(u.Folder.Interval.Duration); err != nil {
			u.Print("[ERROR] Folder poller stopped:", err)
		}
	}()
	u.Printf("[Folder] Polling @ %v: %s", u.Folder.Interval, strings.Join(flist, ", "))
}

// newFolderWatcher returns a new folder watcher.
// You must call folders.FSNotify.Close() when you're done with it.
func (u *Unpackerr) newFolderWatcher() (*Folders, error) {
	w := watcher.New()
	w.FilterOps(watcher.Rename, watcher.Move, watcher.Write, watcher.Create)
	w.IgnoreHiddenFiles(true)

	for _, folder := range u.Folders {
		if err := w.Add(folder.Path); err != nil {
			u.Print("[ERROR] Folder (cannot poll):", err)
		}
	}

	fsn, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("fsnotify.NewWatcher: %w", err)
	}

	for _, folder := range u.Folders {
		if err := fsn.Add(folder.Path); err != nil {
			u.Print("[ERROR] Folder (cannot watch):", err)
		}
	}

	return &Folders{
		Interval: u.Folder.Interval.Duration,
		Config:   u.Folders,
		Folders:  make(map[string]*Folder),
		Events:   make(chan *eventData, u.Config.Buffer),
		Updates:  make(chan *xtractr.Response, updateChanBuf),
		Debugf:   u.Debugf,
		Printf:   u.Printf,
		FSNotify: fsn,
		Watcher:  w,
	}, nil
}

// Add uses either fsnotify or watcher.
func (f *Folders) Add(folder string) error {
	if f.Interval != 0 {
		if err := f.Watcher.Add(folder); err != nil {
			return fmt.Errorf("watcher: %w", err)
		}

		return nil
	}

	if err := f.FSNotify.Add(folder); err != nil {
		return fmt.Errorf("fsnotify: %w", err)
	}

	return nil
}

// Remove uses either fsnotify or watcher.
func (f *Folders) Remove(folder string) {
	if f.Watcher != nil {
		_ = f.Watcher.Remove(folder)
	}

	if f.FSNotify != nil {
		_ = f.FSNotify.Remove(folder)
	}
}

// checkFolders stats all configured folders and returns only "good" ones.
func (u *Unpackerr) checkFolders() ([]*FolderConfig, []string) {
	goodFolders := []*FolderConfig{}
	goodFlist := []string{}

	for _, f := range u.Folders {
		f.Path = strings.TrimSuffix(f.Path, `/\`)
		if stat, err := os.Stat(f.Path); err != nil {
			u.Print("[ERROR] Folder (cannot watch):", err)

			continue
		} else if !stat.IsDir() {
			u.Printf("[ERROR] Folder (cannot watch): %s: not a folder", f.Path)

			continue
		}

		goodFolders = append(goodFolders, f)
		goodFlist = append(goodFlist, f.Path)
	}

	return goodFolders, goodFlist
}

// extractFolder starts a folder's extraction after it hasn't been written to in a while.
func (u *Unpackerr) extractFolder(name string, folder *Folder) {
	// update status.
	u.folders.Remove(name)
	u.folders.Folders[name].last = time.Now()
	u.folders.Folders[name].step = QUEUED
	// create a queue counter in the main history; add to u.Map and send webhook for a new folder.
	u.updateQueueStatus(&newStatus{Name: name, Status: QUEUED}, true)
	u.updateHistory(FolderString + ": " + name)

	// extract it.
	queueSize, err := u.Extract(&xtractr.Xtract{
		Name:       name,
		SearchPath: name,
		TempFolder: !folder.cnfg.MoveBack,
		ExtractTo:  folder.cnfg.ExtractPath,
		DeleteOrig: false,
		CBChannel:  u.folders.Updates,
		CBFunction: nil,
	})
	if err != nil {
		u.Print("[ERROR]", err)

		return
	}

	u.Printf("[Folder] Queued: %s, queue size: %d", name, queueSize)
}

// folderXtractrCallback is run twice by the xtractr library when the extraction begins, and finishes.
func (u *Unpackerr) folderXtractrCallback(resp *xtractr.Response) {
	folder, ok := u.folders.Folders[resp.X.Name]

	switch {
	case !ok:
		// It doesn't exist? weird. delete it and bail out.
		delete(u.Map, resp.X.Name)

		return
	case !resp.Done:
		u.Printf("[Folder] Extraction Started: %s, items in queue: %d", resp.X.Name, resp.Queued)

		folder.step = EXTRACTING
	case resp.Error != nil:
		u.Printf("[Folder] Extraction Error: %s: %v", resp.X.Name, resp.Error)

		folder.step = EXTRACTFAILED
		folder.rars = resp.Archives
	default: // this runs in a go routine
		u.Printf("[Folder] Extraction Finished: %s => elapsed: %v, archives: %d, "+
			"extra archives: %d, files extracted: %d, written: %dMiB",
			resp.X.Name, resp.Elapsed.Round(time.Second), len(resp.Archives),
			len(resp.Extras), len(resp.NewFiles), resp.Size/mebiByte)

		folder.step = EXTRACTED
		folder.rars = resp.Archives
		folder.list = resp.NewFiles
	}

	folder.last = time.Now()

	u.updateQueueStatus(&newStatus{Name: resp.X.Name, Resp: resp, Status: folder.step}, true)
}

// watchFSNotify reads file system events from a channel and processes them.
func (f *Folders) watchFSNotify() {
	for {
		select {
		case err := <-f.Watcher.Error:
			f.Printf("[ERROR] watcher: %v", err)
		case err := <-f.FSNotify.Errors:
			f.Printf("[ERROR] fsnotify: %v", err)
		case event, ok := <-f.FSNotify.Events:
			if !ok {
				return
			}

			f.handleFileEvent(event.Name)
		case event, ok := <-f.Watcher.Event:
			if !ok {
				return
			}

			f.handleFileEvent(event.Name())
		}
	}
}

func (f *Folders) handleFileEvent(name string) {
	if strings.HasSuffix(name, suffix) {
		return
	}

	// Send this event to processEvent().
	for _, cnfg := range f.Config {
		// cnfg.Path: "/Users/Documents/watched_folder"
		// event.Name: "/Users/Documents/watched_folder/new_folder/file.rar"
		// eventData.name: "new_folder"
		if !strings.HasPrefix(name, cnfg.Path) || name == cnfg.Path {
			continue // Not the configured folder for the event we just got.
		} else if p := filepath.Dir(name); p == cnfg.Path {
			f.Events <- &eventData{name: filepath.Base(name), cnfg: cnfg, file: name}
		} else {
			f.Events <- &eventData{name: filepath.Base(p), cnfg: cnfg, file: name}
		}
	}
}

// processEvent processes the event that was received.
func (f *Folders) processEvent(event *eventData) {
	dirPath := filepath.Join(event.cnfg.Path, event.file)
	if stat, err := os.Stat(dirPath); err != nil {
		// Item is unusable (probably deleted), remove it from history.
		if _, ok := f.Folders[dirPath]; ok {
			f.Debugf("Folder: Removing Tracked Item: %v", dirPath)
			delete(f.Folders, dirPath)
			f.Remove(dirPath)
		}

		f.Printf("[ERROR] Folder: Ignored File Event: %v (unreadable: %v)", event.file, err)

		return
	} else if !stat.IsDir() {
		f.Debugf("Folder: Ignoring Item: %v (not a folder)", dirPath)

		return
	}

	if _, ok := f.Folders[dirPath]; ok {
		// f.Debugf("Item Updated: %v", event.file)
		f.Folders[dirPath].last = time.Now()

		return
	}

	if err := f.Add(dirPath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			f.Printf("[ERROR] Folder: Tracking New Item: %v: %v", dirPath, err)
		}

		return
	}

	f.Printf("[Folder] Tracking New Item: %v", dirPath)

	f.Folders[dirPath] = &Folder{
		last: time.Now(),
		step: WAITING,
		cnfg: event.cnfg,
	}
}

// checkFolderStats runs at an interval to see if any folders need work done on them.
// This runs on an interval ticker.
func (u *Unpackerr) checkFolderStats() {
	for name, folder := range u.folders.Folders {
		switch elapsed := time.Since(folder.last); {
		case WAITING == folder.step && elapsed >= u.StartDelay.Duration:
			// The folder hasn't been written to in a while, extract it.
			u.extractFolder(name, folder)
		case EXTRACTFAILED == folder.step && elapsed >= u.RetryDelay.Duration &&
			(u.MaxRetries == 0 || folder.retr < u.MaxRetries):
			u.Retries++
			folder.retr++
			folder.last = time.Now()
			folder.step = WAITING
			u.Printf("[Folder] Re-starting Failed Extraction: %s (%d/%d, failed %v ago)",
				folder.cnfg.Path, folder.retr, u.MaxRetries, elapsed.Round(time.Second))
		case folder.step > EXTRACTING && folder.cnfg.DeleteAfter.Duration <= 0:
			// if DeleteAfter is 0 we don't delete anything. we are done.
			u.updateQueueStatus(&newStatus{Name: name, Status: DELETED, Resp: nil}, false)
			delete(u.folders.Folders, name)
		case EXTRACTED == folder.step && elapsed >= folder.cnfg.DeleteAfter.Duration:
			u.deleteAfterReached(name, folder)
		}
	}
}

// nolint:wsl
func (u *Unpackerr) deleteAfterReached(name string, folder *Folder) {
	var webhook bool

	// Folder reached delete delay (after extraction), nuke it.
	if folder.cnfg.DeleteFiles && !folder.cnfg.MoveBack {
		go u.DeleteFiles(strings.TrimRight(name, `/\`) + suffix)
		webhook = true
	} else if folder.cnfg.DeleteFiles && len(folder.list) > 0 {
		go u.DeleteFiles(folder.list...)
		webhook = true
	}

	if folder.cnfg.DeleteOrig && !folder.cnfg.MoveBack {
		go u.DeleteFiles(name)
		webhook = true
	} else if folder.cnfg.DeleteOrig && len(folder.rars) > 0 {
		go u.DeleteFiles(folder.rars...)
		webhook = true
	}

	u.updateQueueStatus(&newStatus{Name: name, Status: DELETED, Resp: nil}, webhook)
	// Folder reached delete delay (after extraction), nuke it.
	delete(u.folders.Folders, name)
}

type newStatus struct {
	Name   string
	Status ExtractStatus
	Resp   *xtractr.Response
}

// updateQueueStatus for an on-going tracked extraction.
// This is called from a channel callback to update status in a single go routine.
// This is used by apps and Folders in a few other places as well.
func (u *Unpackerr) updateQueueStatus(data *newStatus, sendHook bool) {
	if _, ok := u.Map[data.Name]; !ok {
		// This is a new Folder being queued for extraction.
		// Arr apps do not land here. They create their own queued items in u.Map.
		u.Map[data.Name] = &Extract{
			Path:    data.Name,
			App:     FolderString,
			Status:  QUEUED,
			Updated: time.Now(),
			IDs:     map[string]interface{}{"title": data.Name}, // required or webhook may break.
		}

		if sendHook {
			u.sendWebhooks(u.Map[data.Name])
		}

		return
	}

	if data.Resp != nil {
		u.Map[data.Name].Resp = data.Resp
	}

	u.Map[data.Name].Status = data.Status
	u.Map[data.Name].Updated = time.Now()

	if sendHook {
		u.sendWebhooks(u.Map[data.Name])
	}
}

// only run this once.
func isRunningInDocker() bool {
	// docker creates a .dockerenv file at the root of the container.
	_, err := os.Stat("/.dockerenv")
	return err == nil
}
