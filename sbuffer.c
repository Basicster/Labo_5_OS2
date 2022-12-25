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
    sbuffer_node_t* readers[2];
    bool closed;
    bool sleep[2];

    pthread_mutex_t mutex;
    pthread_cond_t condition;
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
    buffer->sleep[0] = true;
    buffer->sleep[1] = true;
    buffer->readers[0] = NULL;
    buffer->readers[1] = NULL;
    ASSERT_ELSE_PERROR(pthread_mutex_init(&buffer->mutex, NULL) == 0);
    ASSERT_ELSE_PERROR(pthread_cond_init(&buffer->condition, NULL) == 0);

    return buffer;
}

void sbuffer_destroy(sbuffer_t* buffer) {
    assert(buffer);
    // make sure it's empty
    assert(buffer->head == buffer->tail);
    ASSERT_ELSE_PERROR(pthread_mutex_destroy(&buffer->mutex) == 0);
    ASSERT_ELSE_PERROR(pthread_cond_destroy(&buffer->condition) == 0);
    free(buffer);
}

void sbuffer_connmgr(sbuffer_t* buffer, sensor_data_t* dataLocatie) {
    assert(buffer);
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    int ret = sbuffer_insert_first(buffer, dataLocatie);
    assert(ret == SBUFFER_SUCCESS);
    if (buffer->readers[0] == NULL || buffer->readers[1] == NULL) {
        pthread_cond_broadcast(&buffer->condition);
    }
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
}

int sbuffer_read(sbuffer_t* buffer, int defReader, DBCONN* conn) {
    assert(buffer);

    while (buffer->sleep[defReader] && !buffer->closed) {
        pthread_cond_wait(&buffer->condition, &buffer->mutex);
    }
    if (buffer->sleep[defReader]) {
        if (buffer->sleep[0] && buffer->sleep[1]) {
            buffer->readers[defReader] = buffer->tail;
            buffer->sleep[defReader] = false;
        } 
        else {
            buffer->readers[defReader] = buffer->readers[defReader]->prev;
            buffer->sleep[defReader] = false;
            
        }
    }
    
    sensor_data_t* dataLocatie = &(buffer->readers[defReader]->data);
    if (defReader == 1) {
        storagemgr_insert_sensor(conn, dataLocatie->id, dataLocatie->value, dataLocatie->ts);
    } else {
        datamgr_process_reading(dataLocatie);
    }
    if (dataLocatie->delete) {
        sbuffer_remove_last(buffer);
    } else {
        dataLocatie->delete = true;
    }

    sbuffer_node_t* temp = buffer->readers[defReader]->prev;
    if (temp == NULL) {
        buffer->sleep[defReader] = true;
    } else {
        buffer->readers[defReader] = temp;
    }
    
    if (sbuffer_is_empty(buffer) && sbuffer_is_closed(buffer)) {
        return 1;
    } else {
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

void sbuffer_remove_last(sbuffer_t* buffer) {
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
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
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
}

void sbuffer_close(sbuffer_t* buffer) {
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    assert(buffer);
    buffer->closed = true;
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
}
