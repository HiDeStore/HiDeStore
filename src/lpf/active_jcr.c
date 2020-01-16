/*
 * create on: Nov 4, 2019
 *	  Auther: Pengfei Li
 */
#include "active.h"


void init_active_jcr(char *path) {
	active_jcr.path = sdsnew(path);

	struct stat s;
	if (stat(path, &s) != 0) {
		fprintf(stderr, "backup path does not exist: %s!\n", active_jcr.path);
		exit(1);
	}
	if (S_ISDIR(s.st_mode) && active_jcr.path[sdslen(active_jcr.path) - 1] != '/')
		active_jcr.path = sdscat(active_jcr.path, "/");

	active_jcr.bv = NULL;

	active_jcr.id = TEMPORARY_ID;

    active_jcr.status = JCR_STATUS_INIT;

	active_jcr.file_num = 0;
	active_jcr.data_size = 0;
	active_jcr.unique_data_size = 0;
	active_jcr.chunk_num = 0;
	active_jcr.unique_chunk_num = 0;
	active_jcr.zero_chunk_num = 0;
	active_jcr.zero_chunk_size = 0;
	active_jcr.rewritten_chunk_num = 0;
	active_jcr.rewritten_chunk_size = 0;

	active_jcr.sparse_container_num = 0;
	active_jcr.inherited_sparse_num = 0;
	active_jcr.total_container_num = 0;

	active_jcr.total_time = 0;
	/*
	 * the time consuming of seven backup phase
	 */
	active_jcr.read_time = 0;
	active_jcr.chunk_time = 0;
	active_jcr.hash_time = 0;
	active_jcr.dedup_time = 0;
	active_jcr.rewrite_time = 0;
	active_jcr.filter_time = 0;
	active_jcr.write_time = 0;

	/*
	 * the time consuming of three restore phase
	 */
	active_jcr.read_recipe_time = 0;
	active_jcr.read_chunk_time = 0;
	active_jcr.write_chunk_time = 0;

	active_jcr.read_container_num = 0;
}

void init_restore_active_jcr(int revision, char *path) {

	init_active_jcr(path);

	active_jcr.bv = open_active_backup_version(revision);

	if(active_jcr.bv->deleted == 1){
		WARNING("The backup has been deleted!");
		exit(1);
	}

	active_jcr.id = revision;
}

