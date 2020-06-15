import networkx as nx

class ValidateCausalGraph:
    def __init__(self, the_graph):
        # [[(n1, n2, n3, ...), (None, ew1, ew2, ...), sorted_edge_weight_path], ...]
        self.ALL_PATHS = []
        self.sorted_ew_to_count = {}
        self.the_graph = the_graph

    def get_route(self, root, in_weight, v_stack, w_stack):
        v_stack.append(root)
        if in_weight:
            w_stack.append(in_weight)

        gchildren = self.the_graph.edges(root, data=True)
        if len(gchildren) == 0:
            the_path = tuple(i for i in v_stack)
            edge_weight_path = tuple(i for i in w_stack)
            sorted_ew_path = '-'.join(map(lambda x: str(x), sorted(list(edge_weight_path))))
            self.ALL_PATHS.append([the_path, edge_weight_path, sorted_ew_path])
            if sorted_ew_path in self.sorted_ew_to_count:
                self.sorted_ew_to_count[sorted_ew_path] += 1
            else:
                self.sorted_ew_to_count[sorted_ew_path] = 1

        for p,c,w in gchildren:
            self.get_route(c, w['weights'], v_stack, w_stack)

        v_stack.pop()
        if w_stack:
            w_stack.pop()

    def get_all_paths_root_to_leaves(self, the_graph):
        self.get_route("ROOT", None, v_stack=[], w_stack=[])
        print(self.sorted_ew_to_count)

if __name__ == "__main__":
    # ln185_path = "ln185.pickle"
    # ln185_path = "ln743.pickle"
    ln926_path = "ln926.pickle"
    the_graph = nx.read_gpickle(ln185_path)
    # print(type(the_graph["ROOT"]))
    # print(the_graph.edges("ROOT", data=True))
    vcg = ValidateCausalGraph(the_graph)
    vcg.get_all_paths_root_to_leaves(the_graph)

