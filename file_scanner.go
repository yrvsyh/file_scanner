package main

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type FileInfo struct {
	Inode   uint64    `gorm:"primarykey"`
	Name    string    `gorm:"index"`
	Path    string    `gorm:"index"`
	Size    int64     `gorm:"index"`
	ModTime time.Time `gorm:"index"`
	Hash    string    `gorm:"index"`
}

func (f *FileInfo) String() string {
	return fmt.Sprintf("%d %s", f.Size, f.Name)
}

type FileScanner struct {
	Root            string
	DB              *gorm.DB
	dbFileInfoMap   map[uint64]*FileInfo
	currFileInfoMap map[uint64]*FileInfo
}

func NewFileScanner(root string, database string, initData bool, showSql bool) (*FileScanner, error) {
	logLevel := logger.Silent
	if showSql {
		logLevel = logger.Info
	}
	db, err := gorm.Open(sqlite.Open(database), &gorm.Config{
		Logger: logger.Default.LogMode(logLevel),
	})
	if err != nil {
		return nil, err
	}

	if initData {
		db.Exec("DROP TABLE IF EXISTS `file_infos`")
	}

	db.AutoMigrate(&FileInfo{})

	fileScanner := &FileScanner{
		Root:            root,
		DB:              db,
		dbFileInfoMap:   make(map[uint64]*FileInfo),
		currFileInfoMap: make(map[uint64]*FileInfo),
	}

	fileScanner.loadFileInfoFromDB()

	return fileScanner, nil
}

func (s *FileScanner) loadFileInfoFromDB() error {
	var fileInfoList []*FileInfo

	ret := s.DB.Find(&fileInfoList)
	if ret.Error != nil {
		return ret.Error
	}

	for _, fileInfo := range fileInfoList {
		s.dbFileInfoMap[fileInfo.Inode] = fileInfo
	}

	return nil
}

func (s *FileScanner) Scan() error {
	err := filepath.WalkDir(s.Root, s.walkDir)
	if err != nil {
		return err
	}

	return nil
}

func (s *FileScanner) walkDir(path string, entry os.DirEntry, err error) error {
	if err != nil {
		return err
	}

	if entry.Type().IsRegular() {

		var stat syscall.Stat_t
		err := syscall.Stat(path, &stat)
		if err != nil {
			return err
		}

		fileInfo := &FileInfo{
			Inode:   stat.Ino,
			Name:    entry.Name(),
			Path:    path,
			Size:    stat.Size,
			ModTime: time.Unix(stat.Mtim.Unix()),
		}

		s.currFileInfoMap[fileInfo.Inode] = fileInfo
	}

	return nil
}

func (s *FileScanner) UpdateDB() error {
	return s.UpdateDBWithHashSizeLimit(10 * 1024 * 1024)
}

func (s *FileScanner) UpdateDBWithHashSizeLimit(hashSizeLimit int64) error {
	return s.DB.Transaction(func(tx *gorm.DB) error {

		var needDeleteFileInfoList []*FileInfo

		for inode, fileInfo := range s.dbFileInfoMap {
			if _, ok := s.currFileInfoMap[inode]; !ok {
				needDeleteFileInfoList = append(needDeleteFileInfoList, fileInfo)
			}
		}

		if len(needDeleteFileInfoList) > 0 {
			if err := tx.Delete(needDeleteFileInfoList).Error; err != nil {
				return err
			}
		}

		for inode, fileInfo := range s.currFileInfoMap {
			path := fileInfo.Path
			size := fileInfo.Size

			dbFileInfo, ok := s.dbFileInfoMap[inode]
			if ok {
				if fileInfo.ModTime.After(dbFileInfo.ModTime) {
					if size <= hashSizeLimit {
						hash, _ := GetFileHash(path)
						fileInfo.Hash = hash
					}
					if err := tx.Model(fileInfo).Updates(fileInfo).Error; err != nil {
						return err
					}
				}
			} else {
				if size <= hashSizeLimit {
					hash, _ := GetFileHash(path)
					fileInfo.Hash = hash
				}
				if err := tx.Create(fileInfo).Error; err != nil {
					return err
				}
			}
		}

		return nil
	})
}
