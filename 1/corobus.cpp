#include "corobus.h"

#include "libcoro.h"
#include "rlist.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

/**
 * One coroutine waiting to be woken up in a list of other
 * suspended coros.
 */
struct wakeup_entry {
	struct rlist base;
	struct coro *coro;
};

/** A queue of suspended coros waiting to be woken up. */
struct wakeup_queue {
	struct rlist coros;
};

/** Suspend the current coroutine until it is woken up. */
static void
wakeup_queue_suspend_this(struct wakeup_queue *queue)
{
	struct wakeup_entry entry;
	entry.coro = coro_this();
	rlist_add_tail_entry(&queue->coros, &entry, base);
	coro_suspend();
	rlist_del_entry(&entry, base);
}

/** Wakeup the first coroutine in the queue. */
static void
wakeup_queue_wakeup_first(struct wakeup_queue *queue)
{
	if (rlist_empty(&queue->coros))
		return;
	struct wakeup_entry *entry = rlist_first_entry(&queue->coros,
		struct wakeup_entry, base);
	coro_wakeup(entry->coro);
}

/** Wakeup all coroutines in the queue and detach their entries. */
static void
wakeup_queue_wakeup_all(struct wakeup_queue *queue)
{
	while (!rlist_empty(&queue->coros)) {
		struct wakeup_entry *entry =
			rlist_first_entry(&queue->coros,
					  struct wakeup_entry, base);
		rlist_del_entry(entry, base);
		coro_wakeup(entry->coro);
	}
}

struct coro_bus_channel {
	/** Channel max capacity. */
	size_t size_limit;
	/** Coroutines waiting until the channel is not full. */
	struct wakeup_queue send_queue;
	/** Coroutines waiting until the channel is not empty. */
	struct wakeup_queue recv_queue;
	/** Message queue. */
	unsigned *data;
	size_t head;
	size_t size;
};

struct coro_bus {
	struct coro_bus_channel **channels;
	int channel_count;
};

static enum coro_bus_error_code global_error = CORO_BUS_ERR_NONE;

enum coro_bus_error_code
coro_bus_errno(void)
{
	return global_error;
}

void
coro_bus_errno_set(enum coro_bus_error_code err)
{
	global_error = err;
}

static void
channel_init(struct coro_bus_channel *channel, size_t size_limit)
{
	channel->size_limit = size_limit;
	channel->data = size_limit > 0 ? new unsigned[size_limit] : NULL;
	channel->head = 0;
	channel->size = 0;
	rlist_create(&channel->send_queue.coros);
	rlist_create(&channel->recv_queue.coros);
}

static void
channel_destroy(struct coro_bus_channel *channel)
{
	delete[] channel->data;
	channel->data = NULL;
	channel->size_limit = 0;
	channel->head = 0;
	channel->size = 0;
}

struct coro_bus *
coro_bus_new(void)
{
	struct coro_bus *bus = (decltype(bus))malloc(sizeof(*bus));
	if (bus == NULL)
		return NULL;
	bus->channels = NULL;
	bus->channel_count = 0;
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return bus;
}

void
coro_bus_delete(struct coro_bus *bus)
{
	if (bus == NULL)
		return;
	for (int i = 0; i < bus->channel_count; ++i) {
		if (bus->channels[i] != NULL)
			coro_bus_channel_close(bus, i);
	}
	free(bus->channels);
	free(bus);
}

int
coro_bus_channel_open(struct coro_bus *bus, size_t size_limit)
{
	for (int i = 0; i < bus->channel_count; ++i) {
		if (bus->channels[i] == NULL) {
			bus->channels[i] = new coro_bus_channel();
			channel_init(bus->channels[i], size_limit);
			coro_bus_errno_set(CORO_BUS_ERR_NONE);
			return i;
		}
	}
	int new_index = bus->channel_count;
	int new_count = bus->channel_count + 1;
	struct coro_bus_channel **new_channels =
		(struct coro_bus_channel **)realloc(bus->channels,
			new_count * sizeof(*bus->channels));
	if (new_channels == NULL)
		return -1;
	bus->channels = new_channels;
	bus->channel_count = new_count;
	bus->channels[new_index] = new coro_bus_channel();
	channel_init(bus->channels[new_index], size_limit);
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return new_index;
	/*
	 * One of the tests will force you to reuse the channel
	 * descriptors. It means, that if your maximal channel
	 * descriptor is N, and you have any free descriptor in
	 * the range 0-N, then you should open the new channel on
	 * that old descriptor.
	 *
	 * A more precise instruction - check if any of the
	 * bus->channels[i] with i = 0 -> bus->channel_count is
	 * free (== NULL). If yes - reuse the slot. Don't grow the
	 * bus->channels array, when have space in it.
	 */
	return -1;
}

void
coro_bus_channel_close(struct coro_bus *bus, int channel)
{
	if (channel < 0 || channel >= bus->channel_count)
		return;
	struct coro_bus_channel *ch = bus->channels[channel];
	if (ch == NULL)
		return;
	wakeup_queue_wakeup_all(&ch->send_queue);
	wakeup_queue_wakeup_all(&ch->recv_queue);
	bus->channels[channel] = NULL;
	channel_destroy(ch);
	delete ch;
	/*
	 * Be very attentive here. What happens, if the channel is
	 * closed while there are coroutines waiting on it? For
	 * example, the channel was empty, and some coros were
	 * waiting on its recv_queue.
	 *
	 * If you wakeup those coroutines and just delete the
	 * channel right away, then those waiting coroutines might
	 * on wakeup try to reference invalid memory.
	 *
	 * Can happen, for example, if you use an intrusive list
	 * (rlist), delete the list itself (by deleting the
	 * channel), and then the coroutines on wakeup would try
	 * to remove themselves from the already destroyed list.
	 *
	 * Think how you could address that. Remove all the
	 * waiters from the list before freeing it? Yield this
	 * coroutine after waking up the waiters but before
	 * freeing the channel, so the waiters could safely leave?
	 */
}

int
coro_bus_send(struct coro_bus *bus, int channel, unsigned data)
{
	while (true) {
		int rc = coro_bus_try_send(bus, channel, data);
		if (rc == 0) {
			struct coro_bus_channel *ch = NULL;
			if (channel >= 0 && channel < bus->channel_count)
				ch = bus->channels[channel];
			if (ch != NULL && ch->size < ch->size_limit)
				wakeup_queue_wakeup_first(&ch->send_queue);
			return 0;
		}
		enum coro_bus_error_code err = coro_bus_errno();
		if (err != CORO_BUS_ERR_WOULD_BLOCK)
			return -1;
		if (channel < 0 || channel >= bus->channel_count ||
		    bus->channels[channel] == NULL) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}
		struct coro_bus_channel *ch = bus->channels[channel];
		wakeup_queue_suspend_this(&ch->send_queue);
	}
}

int
coro_bus_try_send(struct coro_bus *bus, int channel, unsigned data)
{
	if (channel < 0 || channel >= bus->channel_count ||
	    bus->channels[channel] == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	struct coro_bus_channel *ch = bus->channels[channel];
	if (ch->size == ch->size_limit) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	size_t pos = ch->size_limit > 0 ?
		(ch->head + ch->size) % ch->size_limit : 0;
	if (ch->size_limit > 0)
		ch->data[pos] = data;
	++ch->size;
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	wakeup_queue_wakeup_first(&ch->recv_queue);
	return 0;
}

int
coro_bus_recv(struct coro_bus *bus, int channel, unsigned *data)
{
	while (true) {
		int rc = coro_bus_try_recv(bus, channel, data);
		if (rc == 0) {
			struct coro_bus_channel *ch = NULL;
			if (channel >= 0 && channel < bus->channel_count)
				ch = bus->channels[channel];
			if (ch != NULL && ch->size > 0)
				wakeup_queue_wakeup_first(&ch->recv_queue);
			return 0;
		}
		enum coro_bus_error_code err = coro_bus_errno();
		if (err != CORO_BUS_ERR_WOULD_BLOCK)
			return -1;
		if (channel < 0 || channel >= bus->channel_count ||
		    bus->channels[channel] == NULL) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}
		struct coro_bus_channel *ch = bus->channels[channel];
		wakeup_queue_suspend_this(&ch->recv_queue);
	}
}

int
coro_bus_try_recv(struct coro_bus *bus, int channel, unsigned *data)
{
	if (channel < 0 || channel >= bus->channel_count ||
	    bus->channels[channel] == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	struct coro_bus_channel *ch = bus->channels[channel];
	if (ch->size == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	if (data != NULL && ch->size_limit > 0)
		*data = ch->data[ch->head];
	if (ch->size_limit > 0)
		ch->head = (ch->head + 1) % ch->size_limit;
	--ch->size;
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	wakeup_queue_wakeup_first(&ch->send_queue);
	return 0;
}


#if NEED_BROADCAST

int
coro_bus_broadcast(struct coro_bus *bus, unsigned data)
{
	while (true) {
		int rc = coro_bus_try_broadcast(bus, data);
		if (rc == 0)
			return 0;
		enum coro_bus_error_code err = coro_bus_errno();
		if (err != CORO_BUS_ERR_WOULD_BLOCK)
			return -1;
		/* Wait on any full channel. */
		int wait_idx = -1;
		for (int i = 0; i < bus->channel_count; ++i) {
			struct coro_bus_channel *ch = bus->channels[i];
			if (ch != NULL && ch->size == ch->size_limit) {
				wait_idx = i;
				break;
			}
		}
		if (wait_idx < 0)
			continue;
		struct coro_bus_channel *ch = bus->channels[wait_idx];
		if (ch == NULL) {
			/* Closed while searching - retry. */
			continue;
		}
		wakeup_queue_suspend_this(&ch->send_queue);
	}
}

int
coro_bus_try_broadcast(struct coro_bus *bus, unsigned data)
{
	int existing = 0;
	for (int i = 0; i < bus->channel_count; ++i) {
		if (bus->channels[i] == NULL)
			continue;
		++existing;
		if (bus->channels[i]->size == bus->channels[i]->size_limit) {
			coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
			return -1;
		}
	}
	if (existing == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	for (int i = 0; i < bus->channel_count; ++i) {
		struct coro_bus_channel *ch = bus->channels[i];
		if (ch == NULL)
			continue;
		size_t pos = ch->size_limit > 0 ?
			(ch->head + ch->size) % ch->size_limit : 0;
		if (ch->size_limit > 0)
			ch->data[pos] = data;
		++ch->size;
		wakeup_queue_wakeup_first(&ch->recv_queue);
	}
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return 0;
}

#endif

#if NEED_BATCH

int
coro_bus_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count)
{
	while (true) {
		int rc = coro_bus_try_send_v(bus, channel, data, count);
		if (rc >= 0) {
			/* Wake another sender if there is still space. */
			struct coro_bus_channel *ch = NULL;
			if (channel >= 0 && channel < bus->channel_count)
				ch = bus->channels[channel];
			if (ch != NULL && ch->size < ch->size_limit)
				wakeup_queue_wakeup_first(&ch->send_queue);
			return rc;
		}
		enum coro_bus_error_code err = coro_bus_errno();
		if (err != CORO_BUS_ERR_WOULD_BLOCK)
			return -1;
		if (channel < 0 || channel >= bus->channel_count ||
		    bus->channels[channel] == NULL) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}
		struct coro_bus_channel *ch = bus->channels[channel];
		wakeup_queue_suspend_this(&ch->send_queue);
	}
}

int
coro_bus_try_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count)
{
	if (channel < 0 || channel >= bus->channel_count ||
	    bus->channels[channel] == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	struct coro_bus_channel *ch = bus->channels[channel];
	if (ch->size == ch->size_limit) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	size_t free_space = ch->size_limit - ch->size;
	size_t to_send = count;
	if (to_send > free_space)
		to_send = free_space;
	for (size_t i = 0; i < to_send; ++i) {
		size_t pos = (ch->head + ch->size) % ch->size_limit;
		ch->data[pos] = data[i];
		++ch->size;
	}
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	if (to_send > 0)
		wakeup_queue_wakeup_first(&ch->recv_queue);
	return (int)to_send;
}

int
coro_bus_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity)
{
	while (true) {
		int rc = coro_bus_try_recv_v(bus, channel, data, capacity);
		if (rc >= 0) {
			struct coro_bus_channel *ch = NULL;
			if (channel >= 0 && channel < bus->channel_count)
				ch = bus->channels[channel];
			if (ch != NULL && ch->size > 0)
				wakeup_queue_wakeup_first(&ch->recv_queue);
			return rc;
		}
		enum coro_bus_error_code err = coro_bus_errno();
		if (err != CORO_BUS_ERR_WOULD_BLOCK)
			return -1;
		if (channel < 0 || channel >= bus->channel_count ||
		    bus->channels[channel] == NULL) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}
		struct coro_bus_channel *ch = bus->channels[channel];
		wakeup_queue_suspend_this(&ch->recv_queue);
	}
}

int
coro_bus_try_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity)
{
	if (channel < 0 || channel >= bus->channel_count ||
	    bus->channels[channel] == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	struct coro_bus_channel *ch = bus->channels[channel];
	if (ch->size == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	size_t to_recv = capacity;
	if (to_recv > ch->size)
		to_recv = ch->size;
	for (size_t i = 0; i < to_recv; ++i) {
		data[i] = ch->data[ch->head];
		ch->head = (ch->head + 1) % ch->size_limit;
	}
	ch->size -= to_recv;
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	wakeup_queue_wakeup_first(&ch->send_queue);
	return (int)to_recv;
}

#endif
