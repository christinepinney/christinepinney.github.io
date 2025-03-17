#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <iostream>
#include "lab.h"

#define UNUSED(x) (void)x

static node_t* newNode(void *data)
{
    node_t *rval = (node_t*) malloc(sizeof(node_t));
    memset(rval, 0, sizeof(node_t));
    rval->next = rval;
    rval->prev = rval;
    rval->data = data;
    return rval;
}

list_t *list_init(void (*destroy_data)(void *), int (*compare_to)(const void *, const void *))
{
    // the star makes it heap allocated instead of stack allocated - we want heap
    list_t *rval = (list_t*) malloc(sizeof(list_t)); // no star here because we want the size of the struct not the pointer
    memset(rval, 0, sizeof(list_t)); // we need to zero out structs!!
    rval->head = newNode(NULL);
    rval->destroy_data = destroy_data; // assigning a pointer, not a string
    rval->compare_to = compare_to;
    rval->size = 0;

    return rval;
}

void list_destroy(list_t **list)
{
    list_t *l = *list;
    // only destroy list if it has elements to destory
    if (l->size > 0) {
        node_t *curr = l->head->next;
        node_t *tmp;
        // loop through to destory data of each element and free node
        while (curr != l->head)
        {
            l->destroy_data(curr->data);
            tmp = curr;
            curr = curr->next;
            free(tmp);
        }
        // free the head and list from memory
        free(l->head);
        free(l);
    }
}

list_t *list_add(list_t *list, void *data)
{
    // create a new node
    node_t* n = newNode(data);
    n->next = list->head->next;
    // if this is the first node, we have to handle the sentinel node's reflexive links
    if (list->size == 0) {
        n->prev = list->head->prev;
    } else { // after the initial node is added, new nodes's prev is the head
        n->prev = list->head->prev->next;
    }
    // adjust the exisiting links to include new node
    list->head->next->prev = n;
    list->head->next = n;

    // increment list size
    list->size++;

    return NULL;
}

void *list_remove_index(list_t *list, size_t index)
{
    // if index is invalid, return NULL
    if (index >= list->size) {
        return NULL;
    }
    // move through nodes to get to specified index
    node_t *curr = list->head->next;
    size_t i = 0;
    while (i < index)
    {
        curr = curr->next;
        i++;
    }
    // fix links between nodes
    curr->prev->next = curr->next;
    curr->next->prev = curr->prev;
    // remove node
    list->destroy_data(curr->data);
    free(curr);

    // decrement list size
    list->size--;

    return NULL;
}

int list_indexof(list_t *list, void *data)
{
    node_t *curr = list->head->next;
    int i = 0;
    int j = list->size;
    // loop through list to check nodes for specified data
    while (i < j)
    {
        // use compare_to() to check node data, return index if they match
        if (list->compare_to(curr->data, data) == 0) {
            return i;
        } else {
            curr = curr->next;
            i++;
        }
    }
    return -1;
}

int go(int argc, char **argv)
{
    UNUSED(argc);
    UNUSED(argv);
    printf("in the go function\n");
    return 0;
}
