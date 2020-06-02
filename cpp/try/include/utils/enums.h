#ifndef LIBTRY_ENUMS_H
#define LIBTRY_ENUMS_H

namespace enums {
    enum class Event {
        New,
        Modification,
        Delete
    };

    enum class Autoresolve {
        FullResolve,
        BranchConflicts,
        BranchExternalPush
    };

    enum class RequestType {
        Pull,
        Push
    };

    enum class StatusCode {
        Success = 200,
        GeneralException = 400,
        Timeout = 401
    };

    namespace transfer_fields {
        constexpr char AppName = '0';
        constexpr char Data = '1';
        constexpr char RequestType = '2';
        constexpr char Versions = '3';
        constexpr char Wait = '4';
        constexpr char WaitTimeout = '5';
        constexpr char Status = '6';
        constexpr char Types = '7';
    }

}

#endif //LIBTRY_ENUMS_H
