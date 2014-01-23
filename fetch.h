/*-------------------------------------------------------------------------
 *
 * fetch.h
 *	  Fetching data from a local or remote data directory.
 *
 * This file includes the prototypes for functions used to copy files from
 * one data directory to another. The source to copy from can be a local
 * directory (copy method), or a remote PostgreSQL server (libpq fetch
 * method).
 *
 * Portions Copyright (c) 2013 VMware, Inc. All Rights Reserved.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FETCH_H
#define FETCH_H

#include "c.h"

#include "filemap.h"

/*
 * Common interface. Calls the copy or libpq method depending on global
 * config options.
 */
extern void fetchRemoteFileList(void);
extern char *fetchFile(char *filename, size_t *filesize);
extern void executeFileMap(void);

/* in libpq_fetch.c */
extern void libpqConnect(const char *connstr);
extern void libpqProcessFileList(void);
extern void libpq_executeFileMap(filemap_t *map);
extern void libpqGetChangedDataPages(datapagemap_t *pagemap);
extern void libpqGetOtherFiles(void);
extern char *libpqGetFile(const char *filename, size_t *filesize);

/* in copy_fetch.c */
extern void copy_executeFileMap(filemap_t *map);

extern void open_target_file(const char *path, bool trunc);
extern void write_file_range(char *buf, off_t begin, size_t size);
extern void close_target_file(void);

extern char *slurpFile(const char *datadir, const char *path, size_t *filesize);

typedef void (*process_file_callback_t) (const char *path, size_t size, bool isdir);
extern void traverse_datadir(const char *datadir, process_file_callback_t callback);

extern void remove_target_file(const char *path, bool isdir);
extern void truncate_target_file(const char *path, off_t newsize);
extern void create_target_dir(const char *path);
extern void remove_target_dir(const char *path);
extern void check_samefile(int fd1, int fd2);


#endif   /* FETCH_H */
