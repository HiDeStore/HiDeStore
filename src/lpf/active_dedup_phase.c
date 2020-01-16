/*
 * create on: Nov 1, 2019
 *	  Auther: Pengfei Li
 */

#include "../destor.h"
#include "active.h"
#include "../backup.h"
#include "../index/index.h"
#include "../storage/containerstore.h"
#include "../jcr.h"


static pthread_t active_dedup_t;
static int64_t segment_num;


void send_active_segment(struct segment* s) {
	/*
	 * CHUNK_SEGMENT_START and _END are used for
	 * reconstructing the segment in filter phase.
	 */
	struct chunk* ss = new_chunk(0);
	SET_CHUNK(ss, CHUNK_SEGMENT_START);
	sync_queue_push(active_dedup_queue, ss);

	GSequenceIter *end = g_sequence_get_end_iter(s->chunks);
	GSequenceIter *begin = g_sequence_get_begin_iter(s->chunks);
	while(begin != end) {
		struct chunk* c = g_sequence_get(begin);
	
		sync_queue_push(active_dedup_queue, c);
		g_sequence_remove(begin);
		begin = g_sequence_get_begin_iter(s->chunks);
	}

	struct chunk* se = new_chunk(0);
	SET_CHUNK(se, CHUNK_SEGMENT_END);
	sync_queue_push(active_dedup_queue, se);

	s->chunk_num = 0;

}


void *active_dedup_thread(void *arg) {
	struct segment* s = NULL;
	while (1) {
		struct chunk *c = NULL;
		if (destor.simulation_level != SIMULATION_ALL)
			c = sync_queue_pop(hash_queue);
		else
			c = sync_queue_pop(trace_queue);

		/* Add the chunk to the segment. */
		s = segmenting(c);
		if (!s)
			continue;
		/* segmenting success */
		if (s->chunk_num > 0) {
			VERBOSE("Dedup phase: the %lldth segment of %lld chunks", segment_num++,
					s->chunk_num);
			/* Each duplicate chunk will be marked. */
			//active_filter(s);
			statistic_index(s);
		} else {
			VERBOSE("Dedup phase: an empty segment");
		}
		/* Send chunks in the segment to the next phase.
		 * The segment will be cleared. */
		//send_active_segment(s);

		free_segment(s);
		s = NULL;

		if (c == NULL)
			break;
	}

	sync_queue_term(active_dedup_queue);

	return NULL;
}


void start_active_dedup_phase() {
	active_dedup_queue = sync_queue_new(1000);

	pthread_create(&active_dedup_t, NULL, active_dedup_thread, NULL);
}

void stop_active_dedup_phase() {
	pthread_join(active_dedup_t, NULL);
	/*NOTICE("active_dedup phase stops successfully: %d segments of %d chunks on average",
			segment_num, segment_num ? chunk_num / segment_num : 0);*/
}
