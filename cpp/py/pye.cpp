#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <memory>
#include <string>
#include <iostream>
#include <functional>
#include <memory>
#include <limits>

#include "include/repository.h"
#include "include/utils/utils.h"

#include "pyobj_guard.h"


using utils::json;

using str_pred_t = repository::have_custom_merge_t;
using merge_t = repository::custom_merge_function_t;

namespace {
    using pyext_util::pyobj_guard;

    str_pred_t py_str_pred_to_cpp(pyobj_guard && py_func) {
        if (py_func.get() == Py_None) return nullptr;
        return [m_py_func=std::move(py_func)](std::string const & somestr) {
            std::cout << "invoking custom merge with name: " << somestr <<  " also func: " << (long)m_py_func.get() << std::endl;
            std::cout << "invoking custom merge with name: " << somestr <<  " also func: " << (long)m_py_func.get() << std::endl;
            pyobj_guard arglist{
                    Py_BuildValue("(s#)", somestr.data(), somestr.size()),
                    pyobj_guard::adopt_obj
            };
            std::cout << "arglist: " << (long) arglist.get() << std::endl;
            pyobj_guard result{
                    PyObject_CallObject(m_py_func, arglist.extract()),
                    pyobj_guard::adopt_obj
            };
            std::cout << "result is here: " << result.get() << (result.get() == Py_True) << std::endl;
            if (result.get() == NULL)
                throw std::runtime_error("Python function throws an exception");
            return result.get() == Py_True;
        };
    }

    str_pred_t py_str_pred_from_outer(PyObject * outer) {
        if (outer == Py_None) return nullptr;
        return [outer](std::string const & somestr) {
//            pyobj_guard arglist{
//                    Py_BuildValue("(s#)", somestr.data(), somestr.size()),
//                    pyobj_guard::adopt_obj
//            };
            std::cout << "invoking has custom merge with name: " << somestr << std::endl;
            pyobj_guard result{
                    PyObject_CallMethod(outer, "name_pred", "s#", somestr.data(), somestr.size()),
                    pyobj_guard::adopt_obj
            };
            std::cout << "result is here: " << result.get() << (result.get() == Py_True) << std::endl;
            if (result.get() == NULL)
                throw std::runtime_error("Python function throws an exception");
            return result.get() == Py_True;
        };
    }

    merge_t get_merge_t(pyobj_guard && py_func) {
        if (py_func.get() == Py_None) return nullptr;
        return [py_func=std::move(py_func)](std::string const & dtpname,
                                            std::string const & oid,
                                            json const & original_dims,
                                            json const & new_dims,
                                            json const & conflicting_dims,
                                            std::vector<char> const & new_change_cbor,
                                            std::vector<char> const & conflicting_change_cbor){

            std::vector<char> original_dims_cbor;
            if (!original_dims.is_null()) json::to_cbor(original_dims, original_dims_cbor);

            std::vector<char> new_dims_cbor;
            if (!new_dims.is_null()) json::to_cbor(new_dims, new_dims_cbor);

            std::vector<char> conflicting_dims_cbor;
            if (!conflicting_dims.is_null()) json::to_cbor(conflicting_dims, conflicting_dims_cbor);

            pyobj_guard arglist{
                    Py_BuildValue("(s#s#y#y#y#y#y#)",
                                  dtpname.data(), dtpname.size(),
                                  oid.data(), oid.size(),
                                  original_dims.is_null() ? nullptr : original_dims_cbor.data(), original_dims_cbor.size(),
                                  new_dims.is_null() ? nullptr : new_dims_cbor.data(), new_dims_cbor.size(),
                                  conflicting_dims.is_null() ? nullptr : conflicting_dims_cbor.data(), conflicting_dims_cbor.size(),
                                  new_change_cbor.data(), new_change_cbor.size(),
                                  conflicting_change_cbor.data(), conflicting_change_cbor.size()
                    ),
                    pyobj_guard::adopt_obj
            };
            pyobj_guard result{
                    PyObject_CallObject(py_func, arglist),
                    pyobj_guard::adopt_obj
            };
            if (result.get() == NULL)
                throw std::runtime_error("Python function throws an exception");

            char * result_raw_ptr = PyBytes_AsString(result);
            std::size_t result_size = PyBytes_Size(result);

            json result_pair = json::from_cbor(result_raw_ptr, result_size);

            return std::pair<json, json>(std::move(result_pair[0]), std::move(result_pair[1]));
        };
    }
    merge_t get_merge_t_from_outer(PyObject * outer) {
        if (outer == Py_None) return nullptr;
        return [outer](std::string const & dtpname,
                                            std::string const & oid,
                                            json const & original_dims,
                                            json const & new_dims,
                                            json const & conflicting_dims,
                                            std::vector<char> const & new_change_cbor,
                                            std::vector<char> const & conflicting_change_cbor){
            std::cout << "invoking actual custom merge with name: " << dtpname << std::endl;

            std::vector<char> original_dims_cbor;
            if (!original_dims.is_null()) json::to_cbor(original_dims, original_dims_cbor);

            std::vector<char> new_dims_cbor;
            if (!new_dims.is_null()) json::to_cbor(new_dims, new_dims_cbor);

            std::vector<char> conflicting_dims_cbor;
            if (!conflicting_dims.is_null()) json::to_cbor(conflicting_dims, conflicting_dims_cbor);

//            pyobj_guard arglist{
//                    Py_BuildValue("(s#s#y#y#y#y#y#)",
//                                  dtpname.data(), dtpname.size(),
//                                  oid.data(), oid.size(),
//                                  original_dims.is_null() ? nullptr : original_dims_cbor.data(), original_dims_cbor.size(),
//                                  new_dims.is_null() ? nullptr : new_dims_cbor.data(), new_dims_cbor.size(),
//                                  conflicting_dims.is_null() ? nullptr : conflicting_dims_cbor.data(), conflicting_dims_cbor.size(),
//                                  new_change_cbor.data(), new_change_cbor.size(),
//                                  conflicting_change_cbor.data(), conflicting_change_cbor.size()
//                    ),
//                    pyobj_guard::adopt_obj
//            };
            pyobj_guard result{
                    PyObject_CallMethod(outer, "run_merge", "s#s#y#y#y#y#y#",
                                        dtpname.data(), dtpname.size(),
                                        oid.data(), oid.size(),
                                        original_dims.is_null() ? nullptr : original_dims_cbor.data(),
                                        original_dims_cbor.size(),
                                        new_dims.is_null() ? nullptr : new_dims_cbor.data(), new_dims_cbor.size(),
                                        conflicting_dims.is_null() ? nullptr : conflicting_dims_cbor.data(),
                                        conflicting_dims_cbor.size(),
                                        new_change_cbor.data(), new_change_cbor.size(),
                                        conflicting_change_cbor.data(), conflicting_change_cbor.size()
                    ),
                    pyobj_guard::adopt_obj
            };

            std::cout << "result is here: " << result.get() << (result.get() == Py_True) << std::endl;

            if (result.get() == NULL)
                throw std::runtime_error("Python function throws an exception");

            char * result_raw_ptr = PyBytes_AsString(result);
            std::size_t result_size = PyBytes_Size(result);

            json result_pair = json::from_cbor(result_raw_ptr, result_size);

            return std::pair<json, json>(std::move(result_pair[0]), std::move(result_pair[1]));
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

//static PyObject *
//Repository_getstr(RepositoryObject * self, PyObject * argv[], int argc ) {
//    std::cout << argc << std::endl;
//    //nlohmann::json sss = nlohmann::json::from_cbor(PyBytes_AsString(argv[0]));
//    //std::cout << sss;
//
//    std::string aaa{PyBytes_AsString(PyUnicode_AsASCIIString(argv[0]))};
//    std::cout << aaa;
//
//    std::cout << *(self->xxx) << "stored" << std::endl;
//    return NULL;
//}

//static PyObject *
//Repository_pass_function(RepositoryObject * self, PyObject * argv[], int argc) {
//    using pyext_util::pyobj_guard;
//    pyobj_guard py_func{argv[0]};
//    self->func = [py_func = std::move(py_func)](std::string dtpname, json const & obj1, json const & obj2, json const & obj3) {
//        auto bin1 = json::to_cbor(obj1);
//        auto bin2 = json::to_cbor(obj2);
//        auto bin3 = json::to_cbor(obj3);
//        pyobj_guard arglist{
//                Py_BuildValue("(s#, y#, y#, y#)",
//                              dtpname.data(),
//                              dtpname.size(),
//                              reinterpret_cast<const char *>(bin1.data()),
//                              bin1.size(),
//                              reinterpret_cast<const char *>(bin2.data()),
//                              bin2.size(),
//                              reinterpret_cast<const char *>(bin3.data()),
//                              bin3.size()
//                ),
//                pyobj_guard::adopt_obj};
//        pyobj_guard result{
//                PyObject_CallObject(py_func, arglist),
//                pyobj_guard::adopt_obj};
//        if (result.get() == NULL)
//            throw std::runtime_error("Python function throws an exception");
//        auto data_ptr = PyBytes_AsString(result.get());
//        return data_ptr ? json::from_cbor(data_ptr) : json();
//    };
//    Py_RETURN_NONE;

static PyObject * Repository_is_connected(RepositoryObject * self, PyObject * argv[], int argc) {
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

static PyObject * Repository_push(RepositoryObject * self, PyObject * argv[], int argc) {
    if (argc) {
        PyErr_BadArgument();
        return NULL;
    }

    try {
        std::cout << std::string("pusing repo: ").append(std::to_string((long long)self->m_repo.get())).append("\n");
        self->m_repo->push();
    } catch (std::exception & ex) {
        PyErr_SetString(PyExc_Exception, (std::string("exception from push:") + ex.what()).c_str());
        return NULL;
    }

    Py_RETURN_NONE;
}

static PyObject * Repository_fetch(RepositoryObject * self, PyObject * argv[], int argc) {
    if (argc) {
        PyErr_BadArgument();
        return NULL;
    }

    try {
        self->m_repo->fetch();
    } catch (std::exception & ex) {
        PyErr_SetString(PyExc_Exception, (std::string("exception from fetch: ") + ex.what()).c_str());
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject * Repository_fetch_await(RepositoryObject * self, PyObject * argv[], int argc) {
    if (argc != 1) {
        PyErr_BadArgument();
        return NULL;
    }
    double timeout = PyFloat_AsDouble(argv[0]);

    try {
        self->m_repo->fetch_await(timeout);
    } catch (std::exception & ex) {
        PyErr_SetString(PyExc_Exception, (std::string("exception from fetch_await: ") + ex.what()).c_str());
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject * Repository_push_await(RepositoryObject * self, PyObject * argv[], int argc) {
    if (argc) {
        PyErr_BadArgument();
        return NULL;
    }
    try {
        self->m_repo->push_await();
    } catch (std::exception & ex) {
        PyErr_SetString(PyExc_Exception, "exception from push_await");
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject * Repository_start_server(RepositoryObject * self, PyObject * argv[], int argc) {
    if (argc != 2) {
        PyErr_BadArgument();
        return NULL;
    }

    std::cout << std::string("server repo: ").append(std::to_string((long long)self->m_repo.get())).append("\n");
    unsigned long port = PyLong_AsUnsignedLong(argv[0]);
    unsigned long thread_count = PyLong_AsUnsignedLong(argv[1]);
    if (port > std::numeric_limits<unsigned short>::max() || thread_count > std::numeric_limits<unsigned short>::max()) {
        PyErr_BadArgument();
        return NULL;
    }
    try{
        self->m_repo->start_server(port, thread_count);
    } catch (std::exception & ex) {
        PyErr_SetString(PyExc_Exception, std::string("exception from start_server ").append(ex.what()).c_str());
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject * Repository_connect_to(RepositoryObject * self, PyObject * argv[], int argc) {
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

static PyObject * Repository_retrieve_data(RepositoryObject * self, PyObject * argv[], int argc) {
    if (argc != 2) {
        PyErr_BadArgument();
        return NULL;
    }
    std::string app_name = pystr_to_string(argv[0]);

    std::string version = pystr_to_string(argv[1]);

    std::cout << app_name << version << std::endl;

    try {
        auto[data, start_v, end_v] = self->m_repo->get_manager_ref().retrieve_data(app_name, version);
        json packed_data = {std::move(data), std::move(start_v), std::move(end_v)};
        std::vector<char> packed_cbor;
        json::to_cbor(packed_data, packed_cbor);

        return PyBytes_FromStringAndSize(packed_cbor.data(), packed_cbor.size());
    } catch (std::exception & ex) {
        PyErr_SetString(PyExc_Exception, "exception from retrieve_data");
        return NULL;
    }
}

static PyObject * Repository_receive_data(RepositoryObject * self, PyObject * argv[], int argc) {
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
        if (self->m_repo->get_manager_ref().receive_data(app_name, start_v, end_v, std::move(received_diff),
                                                         from_external))
            Py_RETURN_TRUE;
        else
            Py_RETURN_FALSE;
    } catch (std::exception & ex) {
        PyErr_SetString(PyExc_Exception, "exception from receive_data");
        return NULL;
    }
}

static PyObject * Repository_data_sent_confimed(RepositoryObject * self, PyObject * argv[], int argc) {
    if (argc != 3) {
        PyErr_BadArgument();
        return NULL;
    }

    std::string app_name = pystr_to_string(argv[0]);

    std::string start_v = pystr_to_string(argv[1]);

    std::string end_v = pystr_to_string(argv[2]);

    try {
        self->m_repo->get_manager_ref().data_sent_confirmed(app_name, start_v, end_v);
    } catch (std::exception & ex) {
        PyErr_SetString(PyExc_Exception, "exception from data_sent_confirmed");
        return NULL;
    }

    Py_RETURN_NONE;
}

static PyObject * Repository_wait_version(RepositoryObject * self, PyObject * argv[], int argc) {
    using float_sec_t = std::chrono::duration<double>;
    if (argc != 2) {
        PyErr_BadArgument();
        return NULL;
    }

    std::string version = pystr_to_string(argv[0]);

    double timeout = PyFloat_AsDouble(argv[1]);
    try {
        if (self->m_repo->get_manager_ref().wait_graph_change_for(version, float_sec_t(timeout)))
            Py_RETURN_TRUE;
        else
            Py_RETURN_FALSE;
    } catch (std::exception & ex) {
        PyErr_SetString(PyExc_Exception, "exception from wait_version");
        return NULL;
    }
}


static int
Repository_init(PyObject * self, PyObject * args, PyObject *kwds) {
    using pyext_util::pyobj_guard;
    RepositoryObject * actual_self = (RepositoryObject *) self;

    const char* raw_app_name;
    PyObject * raw_fptr_has_merge_pred;
    PyObject * raw_fptr_repository_merge;
    const char* type_info_bytes;
    Py_ssize_t type_info_length;
    unsigned char autoresolve_int;
    int ok = PyArg_ParseTuple(args, "sOy#b", &raw_app_name, &(actual_self->outer), &type_info_bytes, &type_info_length, &autoresolve_int);
    if (!ok) return -1;


    std::string app_name(raw_app_name);
    pyobj_guard py_func_has_merge_pred(raw_fptr_has_merge_pred);
    pyobj_guard py_func_repository_merge(raw_fptr_repository_merge);

    json type_info = json::from_cbor(type_info_bytes, type_info_length);

    str_pred_t has_merge_pred = py_str_pred_from_outer(actual_self->outer);//py_str_pred_to_cpp(std::move(py_func_has_merge_pred));

    if (has_merge_pred) {
        std::string fff = "AAA";
        bool result = has_merge_pred(fff);
        std::cout << "printing calling str pred " << result << std::endl;
    } else
        std::cout << "pred is null" << std::endl;


    merge_t repository_merge = get_merge_t_from_outer(actual_self->outer); //get_merge_t(std::move(py_func_repository_merge));

    enums::Autoresolve autoresolve;
    switch (autoresolve_int) {
        case 0:
            autoresolve = enums::Autoresolve::FullResolve;
            break;
        case 1:
            autoresolve = enums::Autoresolve::BranchConflicts;
            break;
        case 2:
            autoresolve = enums::Autoresolve::BranchExternalPush;
            break;
        default:
            break;
    }
    try {

        std::unique_ptr<repository::Repository> new_repo = std::make_unique<repository::Repository>(app_name, std::move(has_merge_pred),
                                                                                                    std::move(repository_merge), std::move(type_info),
                                                                                                    autoresolve);
        std::cout << std::string("initing repo: ").append(std::to_string((long long)actual_self->m_repo.get())).append(" ").append(std::to_string((long long)new_repo.get())).append(" ").append(std::to_string((long long)actual_self)).append("\n");

        actual_self->m_repo = std::move(new_repo);

        std::cout << std::string("initing repo: ").append(std::to_string((long long)actual_self->m_repo.get())).append(" ").append(std::to_string((long long)actual_self)).append("\n");
    } catch (std::exception & ex) {
        std::cout << "exception during construction: " << ex.what() << std::endl;
        return -1;
    }
    return 0;
}

void Repository_free(PyObject *self) {
    RepositoryObject * actual_self = (RepositoryObject *) self;
    std::cout << "Deleting" << std::endl;
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
    .m_doc = "Example module that creates an extension type.",
    .m_size = -1,
};


PyMODINIT_FUNC
PyInit_repository(void) //This function gets called when importing
{
    //PyObject *m;
    if (PyType_Ready(&RepositoryType) < 0)
        return NULL;

    pyext_util::pyobj_guard m{PyModule_Create(&repositorymodule), pyext_util::pyobj_guard::adopt_obj};
    if (m == NULL)
        return NULL;

    pyext_util::pyobj_guard repositoryTypePtr((PyObject *) &RepositoryType);
    if (PyModule_AddObject(m, "Repository", repositoryTypePtr) < 0) {
        return NULL;
    }

    return m.extract();
}
