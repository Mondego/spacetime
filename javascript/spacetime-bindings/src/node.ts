import { v4 as uuid } from 'uuid';

function get_app(func: Function, types: Set<any>, producer: Set<any>, getter_setter: Set<any>, getter: Set<any>, setter: Set<any>, deleter: Set<any>, threading = true) {
  
}


/**
 * The Node method builds a custom App instance and returns it. The App instance can be customized through input parameters such as its “handler method for push/pull”, its, which dataframe this app is attached to, if it’s to work as a server, then on what port the app is to run, what are the data types supported in this app, whether to use threads/processes, and a few more other less important customization options. Given the way it is written, only the “handler method for push/pull” seems to be mandatory.
 */
function Node(target: Function, appname = null, dataframe = null, server_port = 0,
  Types: Array<any>, Producer: Array<any>, GetterSetter: Array<any>,
  Getter: Array<any>, Setter: Array<any>, Deleter: Array<any>,
  threading = true, instrument = null, dump_graph = null,
  connection_as = "TODO Socket", resolver = null, autoresolve = "TODO autoresolve",
  mem_instrument = false) {

  // TODO: generate typeName in rtypegen as suggested at
  // https://stackoverflow.com/questions/13613524/get-an-objects-class-name-at-runtime
  if (appname === null) {
    appname = target.getTypeName + uuid();
  }

  app_cls = get_app(target, new Set(Types), new Set(Producer), new Set(GetterSetter)
              new Set(Getter), new Set(Setter), new Set(Deleter), threading = threading)



}