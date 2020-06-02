#pragma once

#define PY_SSIZE_T_CLEAN
#include <Python.h>

namespace pyext_util {
    class pyobj_guard {
    public:
        using value_t = PyObject;

        class adopt_t {
        };

        static adopt_t adopt_obj;

        pyobj_guard();

        pyobj_guard(value_t * obj);

        pyobj_guard(value_t * obj, adopt_t);

        pyobj_guard(pyobj_guard && other);

        pyobj_guard(pyobj_guard const & other);

        pyobj_guard & operator=(pyobj_guard && other);

        pyobj_guard & operator=(pyobj_guard const & other);

        operator value_t *() const;

        value_t * get() const;

        value_t * extract();

        ~pyobj_guard();

        operator bool();
    private:
        value_t * obj;
    };
}
