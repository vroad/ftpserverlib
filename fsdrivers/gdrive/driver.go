// Package gdrive allows to access Google Drive files
package gdrive

// Google Drive SDK
//
// General description:
// https://developers.google.com/drive/v3/web/quickstart/go
//
// Searching for file:
// https://developers.google.com/drive/v3/web/search-parameters

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"google.golang.org/api/drive/v3"

	"github.com/fclairamb/ftpserver/server"
	"github.com/fclairamb/ftpserver/server/log"
)

// Driver is the Google Drive Driver
type Driver struct {
	srv             *drive.Service              // Google Drive Service
	fileByName      map[string]*VirtualFileInfo // Listing by name
	fileByNameMutex sync.Mutex                  // Map shall be synchronized
	logger          log.Logger                  // Logger to use
}

// NewGDriveDrv creates a new isntance of the driver
func NewGDriveDrv(srv *drive.Service, logger log.Logger) server.ClientHandlingDriver {
	drv := &Driver{
		srv:        srv,
		fileByName: make(map[string]*VirtualFileInfo),
		logger:     logger,
	}
	drv.fileByName["/"] = &VirtualFileInfo{file: &drive.File{Id: "root", MimeType: mimeFolder}}

	if err := drv.ChangeDirectory(nil, "/"); err != nil {
		drv.logger.Error(
			"msg", "Could not change directory",
			"action", "gdrive.err.change_dir",
			"err", err,
		)
	}

	return drv
}

// ChangeDirectory changes the current working directory
func (drv *Driver) ChangeDirectory(cc server.ClientContext, path string) error {
	drv.logger.Info(
		"msg", "GDrive: Changing directory",
		"action", "gdrive.ChangeDirectory",
		"path", path,
	)

	info := drv.pathToFileInfo(path)

	// drv.listCache()

	if info != nil && info.IsDir() {
		return nil
	}

	return fmt.Errorf("directory %s not found", path)
}

func pathSplit(filePath string) (dirPath, name string) {
	dirPath, name = path.Split(filePath)
	dirPath = dirPath[0 : len(dirPath)-1]

	return
}

// MakeDirectory creates a directory
func (drv *Driver) MakeDirectory(cc server.ClientContext, directory string) error {
	drv.logger.Info(
		"msg", "GDrive: Making a directory",
		"action", "gdrive.MakeDirectory",
		"directory", directory,
	)

	parentPath, name := pathSplit(directory)

	parentInfo := drv.pathToFileInfo(parentPath)

	if parentInfo == nil {
		return fmt.Errorf("cannot get info around parent path %s", parentPath)
	}

	// This is necessary because gdrive doesn't forbid you to create two directory
	// with the same name in the same directory.
	if info, err := drv.GetFileInfo(cc, directory); err == nil && info != nil {
		return errors.New("directory already exists")
	}

	file := &drive.File{Name: name, MimeType: mimeFolder, Parents: []string{parentInfo.file.Id}}

	if _, err := drv.srv.Files.Create(file).Do(); err != nil {
		return err
	}

	return nil
}

// nolint: unused
func (drv *Driver) listCache() {
	for n, f := range drv.fileByName {
		drv.logger.Debug("Cached file", "name", n, "file", f.file.Id)
	}
}

const mimeFolder = "application/vnd.google-apps.folder"

func getPath(cc server.ClientContext, p string) string {
	path := cc.Path()

	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	path += p

	return path
}

func (drv *Driver) listGdriveFiles(fileID string) ([]*VirtualFileInfo, error) {
	r, err := drv.srv.Files.List().PageSize(1000).Q(
		"'" + fileID + "' in parents and " +
			"mimeType != 'application/vnd.google-apps.document' and " +
			"mimeType != 'application/vnd.google-apps.spreadsheet' and " +
			"mimeType != 'application/vnd.google-apps.presentation' and " +
			"mimeType != 'application/vnd.google-apps.drawing' and " +
			"trashed = false" +
			"").Fields("nextPageToken, files(id, name, size, modifiedTime, mimeType)").Do()

	if err != nil {
		return nil, err
	}

	files := make([]*VirtualFileInfo, 0)
	for _, file := range r.Files {
		files = append(files, &VirtualFileInfo{file: file})
	}

	return files, nil
}

func (drv *Driver) pathToFileInfo(filePath string) *VirtualFileInfo {
	drv.logger.Info("gdrive.pathToFileInfo", "path", filePath)

	drv.fileByNameMutex.Lock()
	defer drv.fileByNameMutex.Unlock()

	if info := drv.fileByName[filePath]; info != nil {
		return info
	}

	list := strings.Split(filePath, "/")
	filePath = ""
	previousPath := "/"

	for _, name := range list {
		if !strings.HasSuffix(filePath, "/") {
			filePath += "/"
		}

		filePath += name
		info := drv.fileByName[filePath]
		drv.logger.Debug(
			"msg", "Searching for dir",
			"filePath", filePath,
			"previous", previousPath,
			"info", info,
		)

		if info == nil {
			previousInfo := drv.fileByName[previousPath]
			if previousInfo != nil {
				if files, err := drv.listGdriveFiles(previousInfo.file.Id); err == nil {
					for _, f := range files {
						newPath := path.Join(previousPath, f.file.Name)
						drv.fileByName[newPath] = f
					}
				} else {
					drv.logger.Error("msg", "Cannot list files", "previousPath", previousPath, "err", err)
				}
				info = drv.fileByName[filePath]
			} else {
				return nil
			}
		}

		previousPath = filePath
	}

	if info := drv.fileByName[filePath]; info != nil {
		return info
	}

	return nil
}

// ListFiles lists the files of a directory
func (drv *Driver) ListFiles(cc server.ClientContext, path string) ([]os.FileInfo, error) {
	drv.logger.Info(
		"msg", "GDrive: Listing files",
		"action", "gdrive.ListFiles",
		"path", path,
	)

	info := drv.pathToFileInfo(path)

	if info == nil {
		return nil, fmt.Errorf("couldn't identify a fileId for path=%s", path)
	}

	list, err := drv.listGdriveFiles(info.file.Id)

	if err != nil {
		drv.logger.Info("Problem listing files...", "fileId", info.file.Id, "err", err)
		return nil, err
	}

	files := make([]os.FileInfo, 0)

	drv.fileByNameMutex.Lock()
	defer drv.fileByNameMutex.Unlock()

	for _, f := range list {
		i := f.file

		path := getPath(cc, i.Name)

		drv.logger.Debug(
			"msg", "File listing",
			"action", "gdrive.ListFiles.fileEntry",
			"path", path,
			"name", i.Name,
			"id", i.Id,
			"modifiedTime", i.ModifiedTime,
			"mimeType", i.MimeType,
		)

		v := &VirtualFileInfo{file: i}
		drv.fileByName[path] = v
		files = append(files, v)
	}

	return files, nil
}

// OpenFile opens a file in 3 possible modes: read, write, appending write (use appropriate flags)
func (drv *Driver) OpenFile(cc server.ClientContext, filePath string, flag int) (server.FileStream, error) {
	fileInfo := drv.pathToFileInfo(filePath)

	drv.logger.Info(
		"msg", "GDrive: Opening a file",
		"action", "gdrive.OpenFile",
		"path", filePath,
		"fileInfo", fileInfo,
	)

	if flag&os.O_WRONLY != 0 {
		dirPath, fileName := pathSplit(filePath)
		parentID := "root"

		if parentInfo := drv.pathToFileInfo(dirPath); parentInfo != nil {
			parentID = parentInfo.file.Id
		} else {
			return nil, fmt.Errorf("couldn't find parent path %s", dirPath)
		}

		drv.logger.Debug(
			"action", "gdrive.OpenFile.gotParent",
			"type", "upload",
			"path", filePath,
			"parentID", parentID,
		)

		gFile := &drive.File{Name: fileName}
		var reader io.Reader
		var writer io.WriteCloser
		reader, writer = io.Pipe()

		// Creating a 20MB buffer
		// writer = utils.NewWriterSize(writer, 1024*1024*40)

		file := &VirtualFile{
			drv:             drv,                           // Driver
			info:            &VirtualFileInfo{file: gFile}, // File info
			writer:          writer,                        // piped writer
			writerErrorChan: make(chan error),              // Error channel (for close)
		}

		go func() {
			drv.logger.Info("gdrive.OpenFile", "type", "upload_start")
			var f *drive.File
			var err error
			if fileInfo == nil {
				gFile.Parents = []string{parentID}
				f, err = drv.srv.Files.Create(gFile).Media(reader).Fields("id").Do()
			} else {
				f, err = drv.srv.Files.Update(fileInfo.file.Id, gFile).Media(reader).Fields("id").Do()
			}

			if err == nil {
				drv.logger.Info("Successful upload", "fileId", f.Id)
			} else {
				drv.logger.Error("Problem during the upload", "err", err)
			}
			file.writerErrorChan <- err
		}()

		return file, nil
	}

	if fileInfo == nil {
		drv.logger.Error("Could not get file", "path", filePath, "fileInfo", fileInfo)
		return nil, errors.New("could not find this file")
	}

	var response *http.Response
	var err error

	// nolint: body-close because it shall be handled somewhere else
	if response, err = drv.srv.Files.Get(fileInfo.file.Id).Download(); err != nil {
		drv.logger.Error(
			"msg", "Problem downloading file",
			"path", filePath,
			"fileId", fileInfo.file.Id,
			"err", err,
		)

		return nil, err
	}

	return &VirtualFile{info: fileInfo, httpResponse: response}, nil
}

func (drv *Driver) CanAllocate(cc server.ClientContext, size int) (bool, error) {
	return true, nil
}

func (drv *Driver) GetFileInfo(cc server.ClientContext, path string) (os.FileInfo, error) {
	fileInfo := drv.pathToFileInfo(path)

	drv.logger.Info(
		"msg", "GDrive: Getting file info",
		"action", "gdrive.GetFileInfo",
		"path", path,
		"fileInfo", fileInfo,
	)

	if fileInfo == nil {
		return nil, errors.New("could not find this file")
	}

	return fileInfo, nil
}

func (drv *Driver) ChmodFile(cc server.ClientContext, path string, mode os.FileMode) error {
	return nil
}

func (drv *Driver) DeleteFile(cc server.ClientContext, path string) error {
	drv.logger.Info(
		"msg", "GDrive: Deleting a file",
		"action", "gdrive.DeleteFile",
		"path", path,
	)

	fileInfo := drv.pathToFileInfo(path)
	if fileInfo != nil {
		_, err := drv.srv.Files.Update(fileInfo.file.Id, &drive.File{Trashed: true}).Do()
		drv.fileByNameMutex.Lock()
		defer drv.fileByNameMutex.Unlock()
		delete(drv.fileByName, path)
		return err
		//return drv.srv.Files.Delete(fileInfo.file.Id).Do()
	} else {
		return errors.New("I don't know this file")
	}
}

func (drv *Driver) RenameFile(cc server.ClientContext, from, to string) error {

	drv.logger.Info("gdrive.RenameFile", "fromPath", from, "toPath", to)

	fromParentPath, _ := pathSplit(from)
	fromFile := drv.pathToFileInfo(from)
	toParentPath, toName := pathSplit(to)
	toParentPathInfo := drv.pathToFileInfo(toParentPath)

	if fromFile == nil {
		return fmt.Errorf("could not get info on file %s", from)
	}
	if toParentPathInfo == nil {
		return fmt.Errorf("Could not get info on dir %s", to)
	}

	call := drv.srv.Files.Update(fromFile.file.Id, &drive.File{Name: toName})

	if fromParentPath != toParentPath {
		if fromGFile, err := drv.srv.Files.Get(fromFile.file.Id).Fields("parents").Do(); err == nil {
			call.RemoveParents(fromGFile.Parents[0])
			call.AddParents(toParentPathInfo.file.Id)
		} else {
			return fmt.Errorf("problem fetching file: %s", err)
		}
	}

	_, err := call.Do()
	return err
}

type VirtualFile struct {
	drv             *Driver          // Driver
	info            *VirtualFileInfo // File info
	httpResponse    *http.Response   // http response
	writer          io.WriteCloser   // Writer
	writerErrorChan chan error       // Error during the writing
}

func (f *VirtualFile) Close() error {
	f.drv.logger.Debug(
		"msg", "Closing virtual file",
		"action", "gdrive.VirtualFile.Close",
		"fileId", f.info.file.Id,
	)
	if f.writer != nil {
		f.writer.Close()
	}
	if f.writerErrorChan != nil {
		return <-f.writerErrorChan
	}
	if f.httpResponse != nil {
		return f.httpResponse.Body.Close()
	}
	return nil // Close makes no sense here
}

func (f *VirtualFile) Read(buffer []byte) (int, error) {
	if f.httpResponse == nil {
		f.drv.logger.Error(
			"msg", "Problem reading file",
			"err", "gdrive.VirtualFile.Close",
			"fileId", f.info.file.Id,
		)
		return 0, errors.New("Cannot access file")
	}
	return f.httpResponse.Body.Read(buffer)
}

func (f *VirtualFile) Seek(n int64, w int) (int64, error) {
	return 0, errors.New("Seek not supported")
}

func (f *VirtualFile) Write(buffer []byte) (int, error) {
	if f.writer == nil {
		return 0, errors.New("No writer !")
	} else {
		// drv.logger.Debug("gdrive.Write", "fileId", f.info.file.Id, "bufferLen", len(buffer))
		return f.writer.Write(buffer)
	}
}

type VirtualFileInfo struct {
	file *drive.File
}

func (f *VirtualFileInfo) Name() string {
	return f.file.Name
}

func (f *VirtualFileInfo) Size() int64 {
	return f.file.Size
}

func (f *VirtualFileInfo) Mode() os.FileMode {
	mode := os.FileMode(0666)
	if f.file.MimeType == mimeFolder {
		mode |= os.ModeDir
	}
	return mode
}

func (f *VirtualFileInfo) IsDir() bool {
	return f.file.MimeType == mimeFolder
}

func (f *VirtualFileInfo) ModTime() time.Time {
	modifiedTime, _ := time.Parse(time.RFC3339, f.file.ModifiedTime)
	return modifiedTime
}

func (f *VirtualFileInfo) Sys() interface{} {
	return nil
}
