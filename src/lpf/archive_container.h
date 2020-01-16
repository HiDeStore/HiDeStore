/*
 * create on: Oct 16, 2019
 *	  Auther: Pengfei Li
 */

#ifndef ARCHIVE_CONTAINER_H_
#define ARCHIVE_CONTAINER_H_


#include "../destor.h"


void init_archive_store();
void close_archive_store();

struct container* create_archive_container();
void write_archive_container_async(struct container *c);
void write_archive_container(struct container* c);


// used for restore
struct containerMeta* retrieve_archive_container_meta_by_id(containerid id);
struct container* retrieve_archive_container_by_id(containerid id);
struct lruCache *create_archive_restore_cache();
struct lruCache *create_archive_container_cache();





#endif