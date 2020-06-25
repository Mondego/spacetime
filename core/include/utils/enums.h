#ifndef DATAFRAME_CORE_ENUMS_H
#define DATAFRAME_CORE_ENUMS_H

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
        constexpr char StartVersion = '3';
        constexpr char EndVersion = '4';
        constexpr char Wait = '5';
        constexpr char WaitTimeout = '6';
        constexpr char Status = '7';
        constexpr char Types = '8';
    }

}

#endif //DATAFRAME_CORE_ENUMS_H
