#include <erl_nif.h>
#include <errno.h>

#include "CRoaring/roaring.h"
#include "mylog.h"

static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_TRUE;
static ERL_NIF_TERM ATOM_FALSE;

typedef struct rbm_context_s {
    roaring_bitmap_t* r;
} rbm_context_t;

static void rbm_dtor(ErlNifEnv* env, void* obj) {
    rbm_context_t *p = (rbm_context_t*)obj;
    LOG("destroy rbm: %p -> %p", p, p->r);
    roaring_bitmap_free(p->r);
    p->r = NULL;
}

static int nifload(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info) {
    *priv_data = enif_open_resource_type(env, NULL,
                                         "roaring_bitmap",
                                         rbm_dtor,
                                         ERL_NIF_RT_CREATE|ERL_NIF_RT_TAKEOVER,
                                         NULL);
    if (*priv_data == NULL)
        return ENOMEM;

    ATOM_OK = enif_make_atom(env, "ok");
    ATOM_TRUE = enif_make_atom(env, "true");
    ATOM_FALSE = enif_make_atom(env, "false");

    LOG("nifload...priv_data(%p)-> %p", priv_data, *priv_data);
    return 0;
}

static ERL_NIF_TERM create(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifResourceType* res_type = (ErlNifResourceType*)enif_priv_data(env);
    rbm_context_t *res = (rbm_context_t*)enif_alloc_resource(res_type, sizeof(*res));
    res->r = roaring_bitmap_create_with_capacity(8 * 1024);
    LOG("priv_data => %p, res => %p, res->r => %p", res_type, res, res->r);
    ERL_NIF_TERM ret = enif_make_resource(env, res);
    enif_release_resource(res);
    return ret;
}

static ERL_NIF_TERM deserialize(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifBinary bin;
    roaring_bitmap_t* ro;
    if (argc != 1 ||
        !enif_inspect_binary(env, argv[0], &bin) ||
        ((ro = roaring_bitmap_deserialize(bin.data)) == NULL)) 
        return enif_make_badarg(env);

    ErlNifResourceType* res_type = (ErlNifResourceType*)enif_priv_data(env);
    rbm_context_t *res = (rbm_context_t*)enif_alloc_resource(res_type, sizeof(*res));
    res->r = ro;
    ERL_NIF_TERM ret = enif_make_tuple2(env, ATOM_OK, enif_make_resource(env, res));
    enif_release_resource(res);
    return ret;
}

static ERL_NIF_TERM serialize(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifResourceType* res_type = (ErlNifResourceType*)enif_priv_data(env);
    rbm_context_t *res;
    if (argc != 1 || !enif_get_resource(env, argv[0], res_type, (void**)&res))
        return enif_make_badarg(env);

    size_t sz = roaring_bitmap_size_in_bytes(res->r);
    ERL_NIF_TERM ret;
    char* data = (char*) enif_make_new_binary(env, sz, &ret);
    if (roaring_bitmap_serialize(res->r, data) != sz) {
        LOG("sz: %u", sz);
        return enif_raise_exception(env, enif_make_atom(env, "failed_serialize"));
    }
    return ret;
}

static ERL_NIF_TERM yield_create_of(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifResourceType* res_type = (ErlNifResourceType*)enif_priv_data(env);
    rbm_context_t *res;
    uint32_t len;
    ERL_NIF_TERM head, tail;
    if (argc != 2 ||
        !enif_get_list_length(env, argv[0], &len) ||
        !(enif_is_list(env, argv[0]) && enif_get_list_cell(env, argv[0], &head, &tail)) ||
        !enif_get_resource(env, argv[1], res_type, (void**)&res)) {
        return enif_make_badarg(env);
    }

    ErlNifTime begin = enif_monotonic_time(ERL_NIF_USEC);
    ErlNifTime latest = begin;
    const ErlNifTime MAX_TIMESLICE = 160; //(160us = 0.16ms)
    const uint32_t step = 1000;
    uint32_t val;
    uint32_t count = 0;
    do {
        enif_get_uint(env, head, &val);
        roaring_bitmap_add(res->r, val);
        if (++count % step == step-1) {
            ErlNifTime current = enif_monotonic_time(ERL_NIF_USEC);
            ErlNifTime elapse = current - latest;
            if (enif_consume_timeslice(env, 100*elapse/MAX_TIMESLICE)) {
                ERL_NIF_TERM newargv[2];
                newargv[0] = tail;
                newargv[1] = argv[1];
                return enif_schedule_nif(env, "yield_create_of", 0, yield_create_of, argc, newargv);
            }
            latest = current;
        }
    } while (enif_get_list_cell(env, tail, &head, &tail));

    LOG("priv_data => %p, res => %p, res->r => %p, elapse: %lu(us)", res_type, res, res->r, latest-begin);
    ERL_NIF_TERM ret = argv[1];
    return ret;
}


static ERL_NIF_TERM create_of(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifResourceType* res_type = (ErlNifResourceType*)enif_priv_data(env);
    uint32_t len;
    if (argc != 1 ||
        !enif_get_list_length(env, argv[0], &len)) {
        return enif_make_badarg(env);
    }

    rbm_context_t *res = (rbm_context_t*)enif_alloc_resource(res_type, sizeof(*res));
    res->r = roaring_bitmap_create_with_capacity(8 * 1024);
    ERL_NIF_TERM ret = enif_make_resource(env, res);
    enif_release_resource(res);
    ERL_NIF_TERM newargv[2];
    newargv[0] = argv[0];
    newargv[1] = ret;
    return enif_schedule_nif(env, "yield_create_of", 0, yield_create_of, 2, newargv);
}

static ERL_NIF_TERM equals(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    rbm_context_t *res1;
    rbm_context_t *res2;
    ErlNifResourceType* res_type = (ErlNifResourceType*)enif_priv_data(env);
    if (argc != 2 ||
        !enif_get_resource(env, argv[0], res_type, (void**)&res1) ||
        !enif_get_resource(env, argv[1], res_type, (void**)&res2))
        return enif_make_badarg(env);
    return roaring_bitmap_equals(res1->r, res2->r) ? ATOM_TRUE:ATOM_FALSE;
}

static ERL_NIF_TERM bitmap_union(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    rbm_context_t *res1;
    rbm_context_t *res2;
    ErlNifResourceType* res_type = (ErlNifResourceType*)enif_priv_data(env);
    if (argc != 2 ||
        !enif_get_resource(env, argv[0], res_type, (void**)&res1) ||
        !enif_get_resource(env, argv[1], res_type, (void**)&res2))
        return enif_make_badarg(env);

    rbm_context_t *res = (rbm_context_t*)enif_alloc_resource(res_type, sizeof(*res));
    res->r = roaring_bitmap_or(res1->r, res2->r);
    LOG("priv_data => %p, res => %p, res->r => %p", res_type, res, res->r);
    ERL_NIF_TERM ret = enif_make_tuple2(env, ATOM_OK, enif_make_resource(env, res));
    enif_release_resource(res);
    return ret;
}

static ERL_NIF_TERM bitmap_intersection(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    rbm_context_t *res1;
    rbm_context_t *res2;
    ErlNifResourceType* res_type = (ErlNifResourceType*)enif_priv_data(env);
    if (argc != 2 ||
        !enif_get_resource(env, argv[0], res_type, (void**)&res1) ||
        !enif_get_resource(env, argv[1], res_type, (void**)&res2))
        return enif_make_badarg(env);

    rbm_context_t *res = (rbm_context_t*)enif_alloc_resource(res_type, sizeof(*res));
    res->r = roaring_bitmap_and(res1->r, res2->r);
    LOG("priv_data => %p, res => %p, res->r => %p", res_type, res, res->r);
    ERL_NIF_TERM ret = enif_make_resource(env, res);
    enif_release_resource(res);
    return ret;
}

static ERL_NIF_TERM is_subset(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    rbm_context_t *res1;
    rbm_context_t *res2;
    ErlNifResourceType* res_type = (ErlNifResourceType*)enif_priv_data(env);
    if (argc != 2 ||
        !enif_get_resource(env, argv[0], res_type, (void**)&res1) ||
        !enif_get_resource(env, argv[1], res_type, (void**)&res2))
        return enif_make_badarg(env);
    return roaring_bitmap_is_subset(res1->r, res2->r) ? ATOM_TRUE:ATOM_FALSE;
}

static ERL_NIF_TERM contains(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    rbm_context_t *res;
    uint32_t val;
    ErlNifResourceType* res_type = (ErlNifResourceType*)enif_priv_data(env);
    if (argc != 2 || !enif_get_resource(env, argv[0], res_type, (void**)&res) ||
            !enif_get_uint(env, argv[1], &val))
        return enif_make_badarg(env);
    return roaring_bitmap_contains(res->r, val) ? ATOM_TRUE:ATOM_FALSE;
}

static ERL_NIF_TERM add(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    rbm_context_t *res;
    uint32_t i;
    ErlNifResourceType* res_type = (ErlNifResourceType*)enif_priv_data(env);
    if (argc != 2 || !enif_get_resource(env, argv[0], res_type, (void**)&res) ||
            !enif_get_uint(env, argv[1], &i))
        return enif_make_badarg(env);
    roaring_bitmap_add(res->r, i);
    return argv[0];
}

static ERL_NIF_TERM get_cardinality(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    rbm_context_t *res;
    ErlNifResourceType* res_type = (ErlNifResourceType*)enif_priv_data(env);
    if (argc != 1 || !enif_get_resource(env, argv[0], res_type, (void**)&res))
        return enif_make_badarg(env);
    uint64_t card = roaring_bitmap_get_cardinality(res->r);
    return enif_make_uint64(env, card);
}

static ERL_NIF_TERM statistic(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    rbm_context_t *res;
    ErlNifResourceType* res_type = (ErlNifResourceType*)enif_priv_data(env);
    if (argc != 1 || !enif_get_resource(env, argv[0], res_type, (void**)&res))
        return enif_make_badarg(env);

    roaring_statistics_t stat;
    roaring_bitmap_statistics(res->r, &stat);
    ERL_NIF_TERM list[] = {
        enif_make_tuple2(env, enif_make_atom(env,"n_containers"), enif_make_uint(env, stat.n_containers)),
        enif_make_tuple2(env, enif_make_atom(env,"n_array_containers"), enif_make_uint(env, stat.n_array_containers)),
        enif_make_tuple2(env, enif_make_atom(env,"n_bytes_array_containers"), enif_make_uint(env, stat.n_bytes_array_containers)),
        enif_make_tuple2(env, enif_make_atom(env,"n_values_array_containers"), enif_make_uint(env, stat.n_values_array_containers)),
        enif_make_tuple2(env, enif_make_atom(env,"n_run_containers"), enif_make_uint(env, stat.n_run_containers)),
        enif_make_tuple2(env, enif_make_atom(env,"n_values_run_containers"), enif_make_uint(env, stat.n_values_run_containers)),
        enif_make_tuple2(env, enif_make_atom(env,"n_bytes_run_containers"), enif_make_uint(env, stat.n_bytes_run_containers)),
        enif_make_tuple2(env, enif_make_atom(env,"n_bitset_containers"), enif_make_uint(env, stat.n_bitset_containers)),
        enif_make_tuple2(env, enif_make_atom(env,"n_values_bitset_containers"), enif_make_uint(env, stat.n_values_bitset_containers)),
        enif_make_tuple2(env, enif_make_atom(env,"n_bytes_bitset_containers"), enif_make_uint(env, stat.n_bytes_bitset_containers)),
        enif_make_tuple2(env, enif_make_atom(env,"max_value"), enif_make_uint(env, stat.max_value)),
        enif_make_tuple2(env, enif_make_atom(env,"min_value"), enif_make_uint(env, stat.min_value)),
        enif_make_tuple2(env, enif_make_atom(env,"sum_value"), enif_make_uint64(env, stat.sum_value)),
        enif_make_tuple2(env, enif_make_atom(env,"cardinality"), enif_make_uint64(env, stat.cardinality)),
    };
    //roaring_bitmap_printf_describe(res->r);
    return enif_make_list_from_array(env, list, sizeof(list)/sizeof(list[0]));
}

static ERL_NIF_TERM f(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifResourceType* res_type = (ErlNifResourceType*)enif_priv_data(env);
    void* res;
    enif_get_resource(env, argv[0], res_type, &res);
    LOG("f: objp => %p", res);
    enif_release_resource(res);
    return ATOM_OK;
}

static ErlNifFunc nif_funcs[] = {
    {"f", 1, f},
    {"create", 0, create},
    {"create", 1, create_of},
    {"serialize", 1, serialize},
    {"deserialize", 1, deserialize},
    {"get_cardinality", 1, get_cardinality},
    {"statistic", 1, statistic},
    {"union", 2, bitmap_union},
    {"intersection", 2, bitmap_intersection},
    {"add", 2, add},
    {"contains", 2, contains},
    {"equals", 2, equals},
    {"is_subset", 2, is_subset}
};

ERL_NIF_INIT(ebitmap, nif_funcs, nifload, NULL,NULL,NULL)
