import os
import datetime
from pathlib import Path
import networkx as nx
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
# Requires:
# sudo apt install python3-graphviz python3-pygraphviz python3-pygraphviz-dbg
from networkx.drawing.nx_agraph import graphviz_layout
from pprint import pprint
from collections import OrderedDict
import re

LOGPATH = os.path.join("Logs", "spacetime.log")
STATEDUMP_ROOT = os.path.join("Logs", "statedump")
NOW_STR = datetime.datetime.now().strftime("%y-%m-%d_%H-%M-%S")
TGT_DIR = os.path.join(STATEDUMP_ROOT, NOW_STR)
Path(TGT_DIR).mkdir(parents=True, exist_ok=True)

params = {'legend.fontsize': 'x-large',
          'figure.figsize': (30, 15),
         'axes.labelsize': 'small',
         'axes.titlesize':'x-large',
         'xtick.labelsize':'x-large',
         'ytick.labelsize':'x-large'}
plt.rcParams.update(params)


import random


def hierarchy_pos2(G, root=None, width=1., vert_gap = 0.2, vert_loc = 0, xcenter = 0.5):

    '''
    From Joel's answer at https://stackoverflow.com/a/29597209/2966723.
    Licensed under Creative Commons Attribution-Share Alike

    If the graph is a tree this will return the positions to plot this in a
    hierarchical layout.

    G: the graph (must be a tree)

    root: the root node of current branch
    - if the tree is directed and this is not given,
      the root will be found and used
    - if the tree is directed and this is given, then
      the positions will be just for the descendants of this node.
    - if the tree is undirected and not given,
      then a random choice will be used.

    width: horizontal space allocated for this branch - avoids overlap with other branches

    vert_gap: gap between levels of hierarchy

    vert_loc: vertical location of root

    xcenter: horizontal location of root
    '''
    if not nx.is_tree(G):
        raise TypeError('cannot use hierarchy_pos on a graph that is not a tree')

    if root is None:
        if isinstance(G, nx.DiGraph):
            root = next(iter(nx.topological_sort(G)))  #allows back compatibility with nx version 1.11
        else:
            root = random.choice(list(G.nodes))

    def _hierarchy_pos(G, root, width=1., vert_gap = 0.2, vert_loc = 0, xcenter = 0.5, pos = None, parent = None):
        '''
        see hierarchy_pos docstring for most arguments

        pos: a dict saying where all nodes go if they have been assigned
        parent: parent of this branch. - only affects it if non-directed

        '''

        if pos is None:
            pos = {root:(xcenter,vert_loc)}
        else:
            pos[root] = (xcenter, vert_loc)
        children = list(G.neighbors(root))
        if not isinstance(G, nx.DiGraph) and parent is not None:
            children.remove(parent)
        if len(children)!=0:
            dx = width/len(children)
            nextx = xcenter - width/2 - dx/2
            for child in children:
                nextx += dx
                pos = _hierarchy_pos(G,child, width = dx, vert_gap = vert_gap,
                                    vert_loc = vert_loc-vert_gap, xcenter=nextx,
                                    pos=pos, parent = root)
        return pos


    return _hierarchy_pos(G, root, width, vert_gap, vert_loc, xcenter)

def hierarchy_pos(G, root, levels=None, width=1., height=1.):
    '''If there is a cycle that is reachable from root, then this will see infinite recursion.
       G: the graph
       root: the root node
       levels: a dictionary
               key: level number (starting from 0)
               value: number of nodes in this level
       width: horizontal space allocated for drawing
       height: vertical space allocated for drawing'''
    TOTAL = "total"
    CURRENT = "current"
    def make_levels(levels, node=root, currentLevel=0, parent=None):
        """Compute the number of nodes for each level
        """
        if not currentLevel in levels:
            levels[currentLevel] = {TOTAL : 0, CURRENT : 0}
        levels[currentLevel][TOTAL] += 1
        neighbors = G.neighbors(node)
        for neighbor in neighbors:
            if not neighbor == parent:
                levels =  make_levels(levels, neighbor, currentLevel + 1, node)
        return levels

    def make_pos(pos, node=root, currentLevel=0, parent=None, vert_loc=0):
        dx = 1/levels[currentLevel][TOTAL]
        left = dx/2
        pos[node] = ((left + dx*levels[currentLevel][CURRENT])*width, vert_loc)
        levels[currentLevel][CURRENT] += 1
        neighbors = G.neighbors(node)
        for neighbor in neighbors:
            if not neighbor == parent:
                pos = make_pos(pos, neighbor, currentLevel + 1, node, vert_loc-vert_gap)
        return pos
    if levels is None:
        levels = make_levels({})
    else:
        levels = {l:{TOTAL: levels[l], CURRENT:0} for l in levels}
    vert_gap = height / (max([l for l in levels])+1)
    return make_pos({})


class STLViewer:
    def __init__(self, fullpath, no_of_lines):
        self.vg_name_graph = OrderedDict() # Example: {'producer1': nx.graph1, ...}
        self.uuid_to_name = dict({"ROOT": "ROOT"}) # Example: '27e50ed7-c752-4b62-bd00-e1edd4cf09b6' -> 1
        self.uuid_to_name_next = 0
        self.process_spacetime_log(fullpath, no_of_lines)

    def read(self, line):
        return dict(
            zip(['timestamp', 'component_name', 'log_level', 'message'],
                line.split(' - ')))

    def ensure_vg_exists(self, version_graph_name):
        if not version_graph_name in self.vg_name_graph:
            self.vg_name_graph[version_graph_name] = nx.DiGraph()

    def get_or_create_uuid_to_name(self, key):
        if not key in self.uuid_to_name:
            self.uuid_to_name_next += 1
            self.uuid_to_name[key] = str(self.uuid_to_name_next)

        return self.uuid_to_name[key]

    ### HANDLERS
    def create_version(self, annotated_line):
        version_graph_name = annotated_line['component_name']
        version = annotated_line['message'].split()[-1]
        print(version_graph_name, version)

        self.ensure_vg_exists(version_graph_name)

        the_graph = self.vg_name_graph[version_graph_name]

        version = self.get_or_create_uuid_to_name(version)

        the_graph.add_node(version)
        print(self.vg_name_graph)

    def extract_parent_child(self, commit_line):
        '''
        Example i/p: "Commit: ('ROOT', '27e50ed7-c752-4b62-bd00-e1edd4cf09b6')"
        o/p: ('ROOT', '27e50ed7-c752-4b62-bd00-e1edd4cf09b6')
        '''
        res = re.search(r"Commit:\s+\(\'(.*)\',\s+\'(.*)\'", commit_line)
        return (res.group(1), res.group(2))

    def commit(self, annotated_line):
        version_graph_name = annotated_line['component_name']
        (parent, child) = self.extract_parent_child(annotated_line['message'])

        self.create_version({'component_name': version_graph_name, 'message': child})
        the_graph = self.vg_name_graph[version_graph_name]
        the_graph.add_edge(self.uuid_to_name[parent], self.uuid_to_name[child])

    def accept_push(self, annotated_line):
        transaction_name = annotated_line['component_name']
        accepting_component = transaction_name.split('<')[0].replace('Remote_', '')
        version_graph_name = 'version_graph_' + accepting_component

        the_graph = self.vg_name_graph[version_graph_name]

        msg = annotated_line['message']
        res = re.search("\[\[.*\]\]", msg)
        edge_list = eval(res.group(0))
        for from_v, to_v, delta, eid in edge_list:
            self.create_version({'component_name': version_graph_name, 'message': to_v})
            the_graph.add_edge(self.uuid_to_name[from_v], self.uuid_to_name[to_v])

    def complete_checkout(self, annotated_line):
        pass

    def default_handler(self, annotated_line):
        pass

    def extract_node12_merge(self, message):
        res = re.search(r"Creating merge version of ([^,]+),\s+([^\s]+)\s+as\s+([^\s$]+)", message)
        return res.groups()

    def create_merge(self, annotated_line):
        version_graph_name = annotated_line['component_name']
        (node1, node2, merge_node) = self.extract_node12_merge(annotated_line['message'])
        self.create_version({'component_name': version_graph_name, 'message': merge_node})

        the_graph = self.vg_name_graph[version_graph_name]
        the_graph.add_edge(self.uuid_to_name[node1], self.uuid_to_name[merge_node])
        the_graph.add_edge(self.uuid_to_name[node2], self.uuid_to_name[merge_node])

    def extract_new_old_uuid(self, message):
        res = re.search(r'Setting alias\s+([^ ]+)\s+\<\-\>\s+([^$\s]+)', message)
        return res.groups()

    def set_alias(self, annotated_line):
        new_name, old_name = self.extract_new_old_uuid(annotated_line['message'])
        self.uuid_to_name[new_name] = self.uuid_to_name[old_name]

    def extract_version_to_delete(self, message):
        res = re.search(r'Deleting version\s+([^$\s]+)', message)
        return res.group(1)

    def delete_version(self, annotated_line):
        del_uuid = self.extract_version_to_delete(annotated_line['message'])
        version_graph_name = annotated_line['component_name']
        the_graph = self.vg_name_graph[version_graph_name]
        try:
            the_graph.remove_node(self.uuid_to_name[del_uuid])
        except:
            print("ERROR: Couldn't remove", self.uuid_to_name[del_uuid])


    ### end HANDLER

    def represent(self, annotated_line):
        instr_handler = {
            'Creating version': self.create_version,
            'Creating merge': self.create_merge,
            'Completed CHECKOUT': self.complete_checkout,
            'Setting alias': self.set_alias,
            'Deleting version': self.delete_version,
            'Accept Push': self.accept_push,
            'Commit': self.commit,
            'default': self.default_handler
        }

        for key in instr_handler:
            if annotated_line['message'].startswith(key):
                instr_handler[key](annotated_line)

    def replace_uuids(self, the_title):
        uuids = re.findall(r'\b[0-9a-f]{8}\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\b[0-9a-f]{12}\b', the_title)
        for u in uuids:
            try:
                the_title = the_title.replace(u, self.uuid_to_name[u])
            except:
                pass
        return the_title

    def output(self, annotated_line, count):
        graph_col_len = len(self.vg_name_graph)
        plt.clf()
        for i, (vg_name, graph) in enumerate(self.vg_name_graph.items()):
            # the_title = 'ln' + str(count) + ' '  + vg_name
            the_title = vg_name
            if annotated_line['component_name'] == vg_name or annotated_line['component_name'].lower().startswith('remote_'):
                the_title += ': '
                the_title += annotated_line['message']

            ax = plt.subplot(1, graph_col_len, i+1, title=self.replace_uuids(the_title)[:75])
            ax.locator_params(nbins=3)
            ax.patch.set_edgecolor('black')
            ax.patch.set_linewidth('1')
            ax.spines['top'].set_visible(True)
            ax.spines['bottom'].set_visible(True)
            ax.spines['right'].set_visible(True)
            ax.spines['left'].set_visible(True)
            # pos = hierarchy_pos(graph, "ROOT")
            plt.suptitle('ln'+str(count), fontsize=14)
            pos = graphviz_layout(graph, prog='dot')
            nx.draw(graph, pos=pos, with_labels=True, ax=ax)

        # plt.tight_layout()
        tgt_file = os.path.join(TGT_DIR, str(count) + '.png')
        plt.savefig(tgt_file, dpi=60)
        print('Done with ', tgt_file)
        # plt.show()


    def process_spacetime_log(self, fullpath, no_of_lines=None):
        thelog = open(LOGPATH, 'r')

        count = 0
        for line in thelog:
            # TODO: identify "loop"
            annotated_line = self.read(line)
            if not (annotated_line['component_name'].lower().startswith("version_graph") or
                annotated_line['component_name'].lower().startswith('remote_')):
                count += 1
                if count == no_of_lines:
                    break
                else:
                    continue

            # print(tokens)
            self.represent(annotated_line)
            self.output(annotated_line, count+1)

            # represent present line in graph
            count += 1
            if count == no_of_lines:
                break

        # dump out picture of state


if __name__ == "__main__":
    s = STLViewer("Logs/spacetime.log", None)  # stop processing after 1 lines
