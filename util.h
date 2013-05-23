/*-------------------------------------------------------------------------
 *
 * util.h
 *		Prototypes for functions in util.c
 *
 * Portions Copyright (c) 2013 VMware, Inc. All Rights Reserved.
 *-------------------------------------------------------------------------
 */
#ifndef UTIL_H
#define UTIL_H

extern char *datasegpath(RelFileNode rnode, ForkNumber forknum,
	    BlockNumber segno);

#endif   /* UTIL_H */
