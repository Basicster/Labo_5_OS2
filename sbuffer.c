/**
 * \author Mathieu Erbas
 */

#ifndef _GNU_SOURCE
    #define _GNU_SOURCE
#endif

#include "sbuffer.h"
#include "datamgr.h"

#include "config.h"

#include <assert.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>

typedef struct sbuffer_node {
    struct sbuffer_node* prev;
    sensor_data_t data;
} sbuffer_node_t;

struct sbuffer {
    sbuffer_node_t* head;
    sbuffer_node_t* tail;
    bool closed;
    pthread_rwlock_t rwlock;
    pthread_mutex_t mutex;
};

static sbuffer_node_t* create_node(const sensor_data_t* data) {
    sbuffer_node_t* node = malloc(sizeof(*node));
    *node = (sbuffer_node_t){
        .data = *data,
        .prev = NULL,
    };
    return node;
}

sbuffer_t* sbuffer_create() {
    sbuffer_t* buffer = malloc(sizeof(sbuffer_t));
    // should never fail due to optimistic memory allocation
    assert(buffer != NULL);

    buffer->head = NULL;
    buffer->tail = NULL;
    buffer->closed = false;
    ASSERT_ELSE_PERROR(pthread_rwlock_init(&buffer->rwlock, NULL) == 0);
    ASSERT_ELSE_PERROR(pthread_mutex_init(&buffer->mutex, NULL) == 0);

    return buffer;
}

void sbuffer_destroy(sbuffer_t* buffer) {
    assert(buffer);
    // make sure it's empty
    assert(buffer->head == buffer->tail);
    ASSERT_ELSE_PERROR(pthread_rwlock_destroy(&buffer->rwlock) == 0);
    ASSERT_ELSE_PERROR(pthread_mutex_destroy(&buffer->mutex) == 0);
    free(buffer);
}

int sbuffer_lock(sbuffer_t* buffer, bool connMgrOrNot, bool storageManagerOrNot, sensor_data_t* dataLocatie, DBCONN* conn) {
    assert(buffer);
    if (connMgrOrNot) {
        ASSERT_ELSE_PERROR(pthread_rwlock_wrlock(&buffer->rwlock) == 0);
        int ret = sbuffer_insert_first(buffer, dataLocatie);
        assert(ret == SBUFFER_SUCCESS);
        ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);
        return 0;
    }
    else {
        ASSERT_ELSE_PERROR(pthread_rwlock_rdlock(&buffer->rwlock) == 0);
        if (!sbuffer_is_empty(buffer)) {
            if (storageManagerOrNot && !dataLocatie->strgMgr) {
                storagemgr_insert_sensor(conn, dataLocatie->id, dataLocatie->value, dataLocatie->ts);
                dataLocatie->strgMgr = true;
            }
            else if (!dataLocatie->dataMgr) {
                datamgr_process_reading(dataLocatie);
                dataLocatie->dataMgr = true;
            }
            ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
            if (dataLocatie != NULL) {
                if (dataLocatie->strgMgr && dataLocatie->dataMgr) {
                    sbuffer_remove_last(buffer);
                } 
            } ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
        }
        else if (sbuffer_is_empty(buffer) && sbuffer_is_closed(buffer)) {
            ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);
            return 1;
        }
        ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);
        return 0;
    }
}

bool sbuffer_is_empty(sbuffer_t* buffer) {
    assert(buffer);
    return buffer->head == NULL;
}

bool sbuffer_is_closed(sbuffer_t* buffer) {
    assert(buffer);
    return buffer->closed;
}

int sbuffer_insert_first(sbuffer_t* buffer, sensor_data_t const* data) {
    assert(buffer && data);
    if (buffer->closed)
        return SBUFFER_FAILURE;

    // create new node
    sbuffer_node_t* node = create_node(data);
    assert(node->prev == NULL);

    // insert it
    if (buffer->head != NULL)
        buffer->head->prev = node;
    buffer->head = node;
    if (buffer->tail == NULL)
        buffer->tail = node;

    return SBUFFER_SUCCESS;
}

sensor_data_t* sbuffer_get_last(sbuffer_t* buffer) {
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    assert(buffer);
    assert(buffer->head != NULL);
    sensor_data_t* temp = &(buffer->tail->data);
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
    return temp;
}

void sbuffer_remove_last(sbuffer_t* buffer) {
    assert(buffer);
    assert(buffer->head != NULL);

    sbuffer_node_t* removed_node = buffer->tail;
    assert(removed_node != NULL);
    if (removed_node == buffer->head) {
        buffer->head = NULL;
        assert(removed_node == buffer->tail);
    }
    buffer->tail = removed_node->prev;
    free(removed_node);
}

void sbuffer_close(sbuffer_t* buffer) {
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    assert(buffer);
    buffer->closed = true;
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
}
