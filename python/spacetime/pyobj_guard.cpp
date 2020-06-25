#include "pyobj_guard.h"
#include <iostream>

pyext_util::pyobj_guard::pyobj_guard() : obj(NULL) {
}

pyext_util::pyobj_guard::pyobj_guard(
        pyext_util::pyobj_guard::value_t * obj) : obj(obj) {
    Py_XINCREF(obj);
}

pyext_util::pyobj_guard::pyobj_guard(
        pyext_util::pyobj_guard && other) : obj(other.obj) {
    other.obj = NULL;
}

pyext_util::pyobj_guard::pyobj_guard(
    const pyext_util::pyobj_guard & other) : pyobj_guard(other.obj) {
}

pyext_util::pyobj_guard::pyobj_guard(
    pyext_util::pyobj_guard::value_t * obj, pyext_util::pyobj_guard::adopt_t)
        : obj(obj) {
}

pyext_util::pyobj_guard::~pyobj_guard() {
    Py_CLEAR(obj);
}

pyext_util::pyobj_guard & pyext_util::pyobj_guard::operator=(
        pyext_util::pyobj_guard && other) {
    Py_CLEAR(obj);
    obj = other.obj;
    other.obj = NULL;
    return *this;
}

pyext_util::pyobj_guard & pyext_util::pyobj_guard::operator=(
        const pyext_util::pyobj_guard & other) {
    if (this == &(other)) return *this;
    Py_CLEAR(obj);
    obj = other.obj;
    Py_XINCREF(obj);
    return *this;
}

pyext_util::pyobj_guard::value_t * pyext_util::pyobj_guard::get() const {
    return obj;
}

pyext_util::pyobj_guard::operator value_t *() const {
    return obj;
}

pyext_util::pyobj_guard::value_t * pyext_util::pyobj_guard::extract() {
    value_t * result = obj;
    obj = NULL;
    return result;
}

pyext_util::pyobj_guard::operator bool() {
    return obj != NULL;
}
