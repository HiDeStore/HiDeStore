/*
 * create on: Nov 6, 2019
 *	  Auther: Pengfei Li
 */

#include "active.h"
#include "archive_container.h"
#include "../jcr.h"
#include "../storage/containerstore.h"
#include "../utils/lru_cache.h"
#include "../utils/sync_queue.h"
#include "../restore.h"
#include "../destor.h"
#include "../recipe/recipestore.h"


static pthread_t lru_read_t;


static void* lru_read_chunk_thread(void *arg) {
	struct lruCache *cache = new_lru_cache(destor.restore_cache[1], free_container,
				lookup_fingerprint_in_container);

	struct chunk* c;
	while ((c = sync_queue_pop(restore_recipe_queue))) {

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
			sync_queue_push(restore_chunk_queue, c);
			continue;
		}

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);


		struct cache_entry* ce = active_area_lookup(ac_table, &c->fp);
		struct chunk *rc = NULL;
		if(ce) {
			rc = read_active_chunk(ce);
		} else {
			struct container *con = lru_cache_lookup(cache, &c->fp);
			if(!con) {
				con = retrieve_archive_container_by_id(c->id);
				lru_cache_insert(cache, con, NULL, NULL);
				active_jcr.read_container_num++;
			}
			rc = get_chunk_in_container(con, &c->fp);
		}

		assert(rc);
		TIMER_END(1, active_jcr.read_chunk_time);
		sync_queue_push(restore_chunk_queue, rc);


		active_jcr.data_size += c->size;
		active_jcr.chunk_num++;
		free_chunk(c);
	}

	sync_queue_term(restore_chunk_queue);

	free_lru_cache(cache);

	return NULL;
}

void start_lru_read_chunk_phase() {
	restore_chunk_queue = sync_queue_new(100);
	if(pthread_create(&lru_read_t, NULL, lru_read_chunk_thread, NULL)!=0) {
		WARNING("Error: active_read_t filed");
		exit(0);
	}
}

void stop_lru_read_chunk_phase() {
	pthread_join(lru_read_t, NULL);
	NOTICE("read_chunk phase stops successfully!");
}

