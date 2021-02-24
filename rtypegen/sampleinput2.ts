import { dimension, primarykey, Dimension } from 'rtypes/attributes'

import { Datatype } from 'rtypes/utils/enums'

import { pccSet } from 'rtypes/types/pcc_set'

@pcc_set
export class Counter {

    _pkey: Dimension;
    _count: Dimension;


    pkey: any;
    count: any;


    /∗###  Don't remove these ###∗/
    static dtype: Map<string, any> = new Map();
    dtype(){ return Counter.dtype; }
    transformProps: any;
    /* ### */


    constructor() {
        this._pkey = primarykey(Datatype.STRING)
        this._count = dimension(Datatype.INTEGER)
        this.name = 'datamodel.Counter'
        /* ### don't remove the following ### */
        this.transformProps()
    }


    handle_conflicts(): {}
}

