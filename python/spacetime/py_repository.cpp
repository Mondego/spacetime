#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <memory>
#include <string>
#include <iostream>
#include <functional>
#include <memory>
#include <limits>

#include "../../core/include/repository.h"
#include "../../core/include/utils/utils.h"

#include "pyobj_guard.h"


using utils::json;

using str_pred_t = repository::have_custom_merge_t;
using merge_t = repository::custom_merge_function_t;

namespace {
    using pyext_util::pyobj_guard;

    str_pred_t py_has_merge_func(PyObject * outer) {
        if (outer == Py_None) return nullptr;
        return [outer](std::string const & somestr) {
            PyObject *response = NULL;

            PyGILState_STATE gstate;
            gstate = PyGILState_Ensure();
            response = PyObject_CallMethod(
                outer, "has_merge_func", "s#", somestr.data(), somestr.size());
            if(response == NULL || PyErr_Occurred() != NULL) {
                PyErr_Print();
            }

            pyobj_guard result{
                response, pyobj_guard::adopt_obj
            };

            PyGILState_Release(gstate);
            if (result.get() == NULL)
                throw std::runtime_error("Python function throws an exception");
            return result.get() == Py_True;
        };
    }

    merge_t py_execute_merge_func(PyObject * outer) {
        if (outer == Py_None) return nullptr;
        return [outer](std::string const & dtpname,
                       std::string const & oid,
                       json const & original_dims,
                       json const & new_dims,
                       json const & conflicting_dims,
                       std::vector<char> const & new_change_cbor,
                       std::vector<char> const & conflicting_change_cbor){
            std::vector<char> original_dims_cbor;
            if (!original_dims.is_null()) json::to_cbor(
                original_dims, original_dims_cbor);

            std::vector<char> new_dims_cbor;
            if (!new_dims.is_null()) json::to_cbor(new_dims, new_dims_cbor);

            std::vector<char> conflicting_dims_cbor;
            if (!conflicting_dims.is_null()) json::to_cbor(
                conflicting_dims, conflicting_dims_cbor);

            PyGILState_STATE gstate;
            gstate = PyGILState_Ensure();

            pyobj_guard result{
                PyObject_CallMethod(
                    outer, "run_merge", "s#s#y#y#y#y#y#",
                    dtpname.data(), dtpname.size(),
                    oid.data(), oid.size(),
                    original_dims.is_null() ? 
                        nullptr : original_dims_cbor.data(),
                    original_dims_cbor.size(),
                    new_dims.is_null() ? 
                        nullptr : new_dims_cbor.data(), new_dims_cbor.size(),
                    conflicting_dims.is_null() ? 
                        nullptr : conflicting_dims_cbor.data(),
                    conflicting_dims_cbor.size(),
                    new_change_cbor.data(), new_change_cbor.size(),
                    conflicting_change_cbor.data(),
                    conflicting_change_cbor.size()
                ),
                pyobj_guard::adopt_obj
            };
            PyGILState_Release(gstate);

            if (result.get() == NULL)
                throw std::runtime_error("Python function throws an exception");

            char * result_raw_ptr = PyBytes_AsString(result);
            std::size_t result_size = PyBytes_Size(result);

            json result_pair = json::from_cbor(result_raw_ptr, result_size);

            return std::pair<json, json>(
                std::move(result_pair[0]), std::move(result_pair[1]));
        };
    }

    std::string pystr_to_string(PyObject * py_str) {
        Py_ssize_t string_len;
        const char * string_ptr = PyUnicode_AsUTF8AndSize(py_str, &string_len);
        return std::string(string_ptr, string_len);
    }
}

struct RepositoryObject {
    PyObject_HEAD
    /* Type-specific fields go here. */
    std::unique_ptr<repository::Repository> m_repo;
    PyObject * outer;
} ;

static PyObject * Repository_is_connected(
        RepositoryObject * self, PyObject * argv[], int argc) {
    if (argc) {
        PyErr_BadArgument();
        return NULL;
    }
    try {
        if (self->m_repo->is_connected())
            Py_RETURN_TRUE;
        else
            Py_RETURN_FALSE;
    } catch (std::exception & ex) {
        PyErr_SetString(PyExc_Exception, "exception from is_connected");
        return NULL;
    }
}

static PyObject * Repository_push(
        RepositoryObject * self, PyObject * argv[], int argc) {
    if (argc) {
        PyErr_BadArgument();
        return NULL;
    }

    try {
        self->m_repo->push();
    } catch (std::exception & ex) {
        PyErr_SetString(
            PyExc_Exception,
            (std::string("exception from push:") + ex.what()).c_str());
        return NULL;
    }

    Py_RETURN_NONE;
}

static PyObject * Repository_fetch(
        RepositoryObject * self, PyObject * argv[], int argc) {
    if (argc) {
        PyErr_BadArgument();
        return NULL;
    }

    try {
        self->m_repo->fetch();
    } catch (std::exception & ex) {
        PyErr_SetString(
            PyExc_Exception,
            (std::string("exception from fetch: ") + ex.what()).c_str());
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject * Repository_fetch_await(
        RepositoryObject * self, PyObject * argv[], int argc) {
    if (argc != 1) {
        PyErr_BadArgument();
        return NULL;
    }
    double timeout = PyFloat_AsDouble(argv[0]);

    try {
        self->m_repo->fetch_await(timeout);
    } catch (std::exception & ex) {
        PyErr_SetString(
            PyExc_Exception,
            (std::string("exception from fetch_await: ") + ex.what()).c_str());
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject * Repository_push_await(
        RepositoryObject * self, PyObject * argv[], int argc) {
    if (argc) {
        PyErr_BadArgument();
        return NULL;
    }
    try {
        self->m_repo->push_await();
    } catch (std::exception & ex) {
        PyErr_SetString(
            PyExc_Exception, ex.what());
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject * Repository_start_server(
        RepositoryObject * self, PyObject * argv[], int argc) {
    if (argc != 2) {
        PyErr_BadArgument();
        return NULL;
    }

    unsigned long port = PyLong_AsUnsignedLong(argv[0]);
    unsigned long thread_count = PyLong_AsUnsignedLong(argv[1]);
    if (port > std::numeric_limits<unsigned short>::max()
            || thread_count > std::numeric_limits<unsigned short>::max()) {
        PyErr_BadArgument();
        return NULL;
    }
    try{
        auto bound_port = self->m_repo->start_server(port, thread_count);
        return Py_BuildValue("I", bound_port);
    } catch (std::exception & ex) {
        PyErr_SetString(
            PyExc_Exception, std::string("exception from start_server ").append(
                ex.what()).c_str());
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject * Repository_connect_to(
        RepositoryObject * self, PyObject * argv[], int argc) {
    if (argc != 2) {
        PyErr_BadArgument();
        return NULL;
    }

    Py_ssize_t address_len;
    const char * address_ptr = PyUnicode_AsUTF8AndSize(argv[0], &address_len);

    std::string address(address_ptr, address_len);
    unsigned long port = PyLong_AsUnsignedLong(argv[1]);

    if (port > std::numeric_limits<unsigned short>::max()) {
        PyErr_BadArgument();
        return NULL;
    }

    try{
        self->m_repo->connect_to(address, port);
    } catch (std::exception & ex) {
        PyErr_SetString(PyExc_Exception, "exception from connect_to");
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject * Repository_retrieve_data(
        RepositoryObject * self, PyObject * argv[], int argc) {
    if (argc != 2) {
        PyErr_BadArgument();
        return NULL;
    }
    std::string app_name = pystr_to_string(argv[0]);

    std::string version = pystr_to_string(argv[1]);

    try {
        auto[data, start_v, end_v] =
            self->m_repo->get_manager_ref().retrieve_data(app_name, version);
        json packed_data = 
            {std::move(data), std::move(start_v), std::move(end_v)};
        std::vector<char> packed_cbor;
        json::to_cbor(packed_data, packed_cbor);

        return PyBytes_FromStringAndSize(
            packed_cbor.data(), packed_cbor.size());
    } catch (std::exception & ex) {
        PyErr_SetString(PyExc_Exception, "exception from retrieve_data");
        return NULL;
    }
}

static PyObject * Repository_receive_data(
        RepositoryObject * self, PyObject * argv[], int argc) {
    if (argc != 5) {
        PyErr_BadArgument();
        return NULL;
    }
    std::string app_name = pystr_to_string(argv[0]);

    std::string start_v = pystr_to_string(argv[1]);

    std::string end_v = pystr_to_string(argv[2]);

    char * diff_data_ptr;
    Py_ssize_t diff_data_size;
    if (PyBytes_AsStringAndSize(argv[3], &diff_data_ptr, &diff_data_size) == -1) {
        PyErr_BadArgument();
        return NULL;
    }

    json received_diff = json::from_cbor(diff_data_ptr, diff_data_size);
    bool from_external = argv[4] == Py_True;

    try {
        if (self->m_repo->get_manager_ref().receive_data(
                app_name, start_v, end_v,
                std::move(received_diff), from_external))
            Py_RETURN_TRUE;
        else
            Py_RETURN_FALSE;
    } catch (std::exception & ex) {
        PyErr_SetString(PyExc_Exception, "exception from receive_data");
        return NULL;
    }
}

static PyObject * Repository_data_sent_confimed(
        RepositoryObject * self, PyObject * argv[], int argc) {
    if (argc != 3) {
        PyErr_BadArgument();
        return NULL;
    }

    std::string app_name = pystr_to_string(argv[0]);

    std::string start_v = pystr_to_string(argv[1]);

    std::string end_v = pystr_to_string(argv[2]);

    try {
        self->m_repo->get_manager_ref().data_sent_confirmed(
            app_name, start_v, end_v);
    } catch (std::exception & ex) {
        PyErr_SetString(PyExc_Exception, "exception from data_sent_confirmed");
        return NULL;
    }

    Py_RETURN_NONE;
}

static PyObject * Repository_wait_version(
        RepositoryObject * self, PyObject * argv[], int argc) {
    using float_sec_t = std::chrono::duration<double>;
    if (argc != 2) {
        PyErr_BadArgument();
        return NULL;
    }

    std::string version = pystr_to_string(argv[0]);

    double timeout = PyFloat_AsDouble(argv[1]);
    try {
        if (self->m_repo->get_manager_ref().wait_graph_change_for(
                version, float_sec_t(timeout)))
            Py_RETURN_TRUE;
        else
            Py_RETURN_FALSE;
    } catch (std::exception & ex) {
        PyErr_SetString(PyExc_Exception, "exception from wait_version");
        return NULL;
    }
}


static int Repository_init(PyObject * self, PyObject * args, PyObject *kwds) {
    using pyext_util::pyobj_guard;
    RepositoryObject * actual_self = (RepositoryObject *) self;

    const char* raw_app_name;
    const char* type_info_bytes;
    Py_ssize_t type_info_length;
    int ok = PyArg_ParseTuple(
        args, "sOy#", &raw_app_name, &(actual_self->outer),
        &type_info_bytes, &type_info_length);
    if (!ok) return -1;

    std::string app_name(raw_app_name);

    json type_info = json::from_cbor(type_info_bytes, type_info_length);

    str_pred_t has_merge_pred = py_has_merge_func(actual_self->outer);

    merge_t repository_merge = py_execute_merge_func(actual_self->outer);

    enums::Autoresolve autoresolve = enums::Autoresolve::FullResolve;
    try {

        std::unique_ptr<repository::Repository> new_repo = 
            std::make_unique<repository::Repository>(
                app_name, std::move(has_merge_pred),
                std::move(repository_merge), std::move(type_info),
                autoresolve);

        actual_self->m_repo = std::move(new_repo);

    } catch (std::exception & ex) {
        return -1;
    }
    return 0;
}

void Repository_free(PyObject *self) {
    RepositoryObject * actual_self = (RepositoryObject *) self;
    actual_self->m_repo = nullptr;
}

static PyMethodDef Repository_methods[] = {
        {"push", (PyCFunction) Repository_push, METH_FASTCALL, "push()"},
        {"push_await", (PyCFunction) Repository_push_await, METH_FASTCALL, "push_await()"},
        {"start_server", (PyCFunction) Repository_start_server, METH_FASTCALL, "start_server(long->unsigned short port, long->unsigned short thread_count)"},
        {"connect_to", (PyCFunction) Repository_connect_to, METH_FASTCALL, "connect_to(str->std::string address, long->unsigned short port)"},
        {"fetch", (PyCFunction) Repository_fetch, METH_FASTCALL, "fetch()"},
        {"fetch_await", (PyCFunction) Repository_fetch_await, METH_FASTCALL, "fetch_await(float->double timeout)"},
        {"retrieve_data", (PyCFunction) Repository_retrieve_data, METH_FASTCALL, "retrieve_data(str->std::string app_name, str->std::string version) -> cbor_of(std::tuple<json data, std::string start_v, std::string end_v>)"},
        {"data_sent_confirmed", (PyCFunction) Repository_data_sent_confimed, METH_FASTCALL, "data_sent_confirmed(str->std::string appname, str->std::string start_v, str->std::string end_v)"},
        {"receive_data", (PyCFunction) Repository_receive_data, METH_FASTCALL, "receive_data(str->std::string appname, str->std::string start_v, str->std::string end_v, bytes->json diff_data, bool from_external) -> bool succ"},
        {"wait_version", (PyCFunction) Repository_wait_version, METH_FASTCALL, "wait_version(str->std::string version, double timeout) -> bool succ"},
        {"is_connected", (PyCFunction) Repository_is_connected, METH_FASTCALL, "is_connected() -> bool succ"},
        {NULL}
};

static PyTypeObject RepositoryType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "repository.Repository",
    .tp_basicsize = sizeof(RepositoryObject),
    .tp_itemsize = 0,
    .tp_dealloc = Repository_free,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "Repository objects",
    .tp_methods = Repository_methods,
    .tp_init = Repository_init,
    .tp_new = PyType_GenericNew,
};

static PyModuleDef repositorymodule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "repository",
    .m_doc = "Python interface to Dataframe c++ core.",
    .m_size = -1,
};

PyMODINIT_FUNC PyInit_repository(void) {
    //This function gets called when importing
    if (PyType_Ready(&RepositoryType) < 0)
        return NULL;

    pyext_util::pyobj_guard m{
        PyModule_Create(&repositorymodule), pyext_util::pyobj_guard::adopt_obj};
    if (!m)
        return NULL;

    pyext_util::pyobj_guard repositoryTypePtr((PyObject *) &RepositoryType);
    if (PyModule_AddObject(m, "Repository", repositoryTypePtr) < 0) {
        return NULL;
    }

    return m.extract();
}
