/*-------------------------------------------------------------------------
 *
 * pg_rewind.h
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2013 VMware, Inc. All Rights Reserved.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_REWIND_H
#define PG_REWIND_H

#include "c.h"

#include "datapagemap.h"
#include "util.h"

#include "access/timeline.h"
#include "storage/block.h"
#include "storage/relfilenode.h"

#define PG_REWIND_VERSION "0.1"

/* Configuration options */
extern char *datadir_target;
extern char *datadir_source;
extern char *connstr_source;
extern int verbose;
extern int dry_run;


/* in parsexlog.c */
extern void extractPageMap(const char *datadir, XLogRecPtr startpoint,
						   TimeLineID tli, XLogRecPtr endpoint);
extern void findLastCheckpoint(const char *datadir, XLogRecPtr searchptr, TimeLineID tli,
				   XLogRecPtr *lastchkptrec, TimeLineID *lastchkpttli,
				   XLogRecPtr *lastchkptredo);
extern XLogRecPtr readOneRecord(const char *datadir, XLogRecPtr ptr, TimeLineID tli);


/* in timeline.c */
extern TimeLineHistoryEntry *rewind_parseTimeLineHistory(char *buffer,
		    TimeLineID targetTLI, int *nentries);

#endif   /* PG_REWIND_H */
