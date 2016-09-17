statsdir = Sys.getenv("SPACETIME_STATS")
wd = Sys.getenv("SPACETIME_SCRIPTS")
result_dir = Sys.getenv("SPACETIME_RESULTS")

setwd(wd)

source("parser.R")

results = list()
colors = list()
colors$queue = c('blue','green', 'red', 'orange')
colors$new = 1
mode2colors = list()
runs = list.dirs(statsdir, recursive = FALSE)
for (d in runs) {
    all_files = list.files(d, full.names = TRUE)
    mode = unlist(strsplit(basename(d),"[ ]"))[1]
    for (f in all_files) {
        fname = basename(f)
        no_ext = unlist(strsplit(fname, "[.]"))[1]
        options  = unlist(strsplit(no_ext, "[ ]"))
        testfile = options[1]
        sim = options[2]
        testname = substr(options[3], 1, nchar(options[3])-1)
        instances = substr(options[4], 1, nchar(options[4])-1)
        length = substr(options[5], 1, nchar(options[5])-1)

        if (!is.element(testname, names(results))) {
            results[[testname]] = list()
        }
        if (!is.element(sim, names(results[[testname]]))) {
            results[[testname]][[sim]] = list()
        }
        if (!is.element(instances, names(results[[testname]][[sim]]))) {
            results[[testname]][[sim]][[instances]] = list()
        }
        if (!is.element(mode, names(results[[testname]][[sim]][[instances]]))) {
            results[[testname]][[sim]][[instances]][[mode]] = list()
        }
        if (!is.element(mode, names(mode2colors))) {
            mode2colors[[mode]] = colors$queue[colors$new]
            colors$new = colors$new + 1
        }
        results[[testname]][[sim]][[instances]][[mode]] = benchmark_parser(f, FALSE)
    }
}


for (tname in names(results)) {
    for (sim in names(results[[tname]])) {
        for (ninst in names(results[[tname]][[sim]])) {
            graph = list()
            graph$update_delta = list()
            graph$lgd_names = list()
            graph$lgd_colors = list()
            for (nmode in names(results[[tname]][[sim]][[ninst]])) {
                vals <- results[[tname]][[sim]][[ninst]][[nmode]]
                graph$update_delta <- c(graph$update_delta, list(vals$table$update_delta))
                graph$lgd_names <- c(graph$lgd_names, nmode)
                graph$lgd_colors <- c(graph$lgd_colors, mode2colors[[nmode]])
            }
            generate_graph(graph$update_delta, graph$lgd_names, graph$lgd_colors, paste(result_dir,paste(sim,tname,ninst,".pdf"),sep="/"))
        }
    }
}


