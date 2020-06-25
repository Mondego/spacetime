#include <version_graph.h>

#include <utility>
#include <iostream>
#include <utils/debug_logger.h>

namespace {
    inline static const std::string root_tag = "ROOT";

    template <class T1, class T2>
    std::pair<T1&, T2&> tie_pair(T1 & t1, T2 & t2) noexcept {
        return std::pair<T1&, T2&>(t1, t2);
    }
}

/*
version_graph::Node::Node(std::string tag, bool is_master) :
	tag(std::move(tag)), is_master(is_master) {}

void version_graph::Node::set_next(std::shared_ptr<Node> const & version) {
    auto next_master_shared = next_edge.lock();
    if (!next_master_shared)
        next_edge = version;
    all_next.insert(version);
}

void version_graph::Node::set_prev(std::shared_ptr<Node> const & version) {
    auto prev_master_shared = prev_edge.lock();
    if (!prev_master_shared)
        prev_edge = version;
    all_prev.insert(version);
}

version_graph::Edge::Edge(std::shared_ptr<Node> from_node, std::shared_ptr<Node> to_node, nlohmann::json && payload):
    from_node(std::move(from_node)), to_node(std::move(to_node)), payload(payload) {}

version_graph::Graph::Graph() :
    head(std::make_unique<Node>(root_tag, true)), tail(head), nodes({{root_tag, head}}) {}
*/

void version_graph::Graph::continue_chain(std::string const & from_version, std::string const & to_version, nlohmann::json && payload, bool force_branch) {
    // std::cout << std::string("\ncontinue chain : at ").append(std::to_string((long long)this)).append(", ").append(from_version).append(" to ").append(to_version).append("\n");
    std::shared_ptr<Node> from_node;
    std::shared_ptr<Node> to_node;
    if (tag_to_node.count(from_version)) {
        from_node = tag_to_node[from_version].lock();
    } else {
        from_node = std::make_shared<Node>(from_version, from_version == head->tag, *this);
        std::unique_lock tag_to_node_lock(tag_to_node_mutex);
        tag_to_node.emplace(from_version, from_node);
    }
    if (tag_to_node.count(to_version)) {
        to_node = tag_to_node[to_version].lock();
    } else {
        to_node = std::make_shared<Node>(to_version, !force_branch && from_version == head->tag, *this);
        std::unique_lock tag_to_node_lock(tag_to_node_mutex);
        tag_to_node.emplace(to_version, to_node);
    }
    std::shared_ptr<Edge> edge = std::make_shared<Edge>(from_node, to_node, std::move(payload));
    {
        std::unique_lock from_node_lock(from_node->next_mutex);
        std::unique_lock to_node_lock(to_node->prev_mutex);
        from_node->set_next(edge, from_node);
        to_node->set_prev(edge, to_node);
    }
    {
        std::unique_lock head_lock(head_mutex);
        if (from_node == head)
            head = to_node;
    }
    std::lock_guard disposal_lock(disposal_mutex);
    disposal.emplace(std::move(from_node));
    disposal.emplace(std::move(to_node));

}

void version_graph::Graph::dispose_node(std::shared_ptr<Node> &&node_ptr) {
    std::lock_guard disposal_lock(disposal_mutex);
    disposal.emplace(std::move(node_ptr));
}

void version_graph::Graph::clear_disposal() {
    std::shared_ptr<Node> current_node;
    while (true) {
        {
            std::lock_guard disposal_lock(disposal_mutex);
            if (disposal.empty()) break;
            current_node = disposal.extract(disposal.begin()).value();
        }
        current_node = nullptr;
    }
}

version_graph::Graph::Graph()
: is_dead(false) {
    tail = head = std::make_unique<Node>(root_tag, true, *this);
    tag_to_node.emplace(root_tag, tail);
}

version_graph::Graph::~Graph() {
    is_dead = true;
    disposal.clear();
    external_refs.clear();
    external_refs_await.clear();
    merge_nodes.clear();
    split_nodes.clear();
    tail = head = nullptr;
}

version_graph::Graph::iterator version_graph::Graph::end(std::string const & tag) {
    if (!tag.empty()) {
        std::shared_lock tag_to_node_lock(tag_to_node_mutex);
        return version_graph::Graph::iterator(
                *this, tag_to_node[tag].lock(), version_graph::Graph::iterator::last_it);
    }
    std::shared_lock head_lock(head_mutex);
    return version_graph::Graph::iterator(*this, head, version_graph::Graph::iterator::last_it);
}

version_graph::Graph::iterator version_graph::Graph::begin(std::string const & tag) {
    if (!tag.empty()) {
        std::shared_lock tag_to_node_lock(tag_to_node_mutex);
        return version_graph::Graph::iterator(*this, tag_to_node[tag].lock());
    }
    return version_graph::Graph::iterator(*this, tail);
}


version_graph::Graph::reverse_iterator version_graph::Graph::rend(std::string const & tag) {
    if (!tag.empty()) {
        std::shared_lock tag_to_node_lock(tag_to_node_mutex);
        return version_graph::Graph::reverse_iterator(
                *this, tag_to_node[tag].lock(), version_graph::Graph::reverse_iterator::last_it);
    }
    return version_graph::Graph::reverse_iterator(*this, tail, version_graph::Graph::reverse_iterator::last_it);
}

version_graph::Graph::reverse_iterator version_graph::Graph::rbegin(std::string const & tag) {
    if (!tag.empty()) {
        std::shared_lock tag_to_node_lock(tag_to_node_mutex);
        return version_graph::Graph::reverse_iterator(*this, tag_to_node[tag].lock());
    }
    std::shared_lock head_lock(head_mutex);
    return version_graph::Graph::reverse_iterator(*this, head);
}

version_graph::Graph::Node::~Node() {
    if (graph.is_dead) return;

    {
        std::unique_lock tag_to_node_lock(graph.tag_to_node_mutex);
        graph.tag_to_node.erase(tag);
    }
    if (prev_edge && next_edge) {
        auto prev = prev_edge->prev.lock();
        auto next = next_edge->next.lock();
        {
            std::unique_lock prev_lock(prev->next_mutex);
            std::unique_lock next_lock(next->prev_mutex);
            std::shared_ptr<Edge> merged_edge = nullptr;

            if (prev->next_edge == this->prev_edge || next->prev_edge == this->next_edge) {
                merged_edge = std::make_unique<Edge>(
                        prev,
                        next,
                        utils::merge_state_delta(std::move(prev_edge->payload), std::move(next_edge->payload))
                        /*,prev_edge->payload + next->next_edge->payload*/
                );
            }
            if (prev->next_edge == this->prev_edge) {
                prev->next_edge = merged_edge;
            } else {
                if (!(--prev->next_count))
                    graph.split_nodes.erase(prev);
            }
            if (next->prev_edge == this->next_edge) {
                next->prev_edge = merged_edge;
            } else {
                if (!(--next->prev_count))
                    graph.merge_nodes.erase(next);
            }
        }
    }
}

version_graph::Graph::Node::Node(std::string tag, bool is_master, Graph &graph) :
tag(std::move(tag)), is_master(is_master), graph(graph), prev_count(0), next_count(0) {

}

void
version_graph::Graph::Node::set_prev(std::shared_ptr<Edge> const & prev, std::shared_ptr<Node> const & shared_this) {
    if (prev_edge) {
        if (!(prev_count++)) {
            graph.merge_nodes.emplace(shared_this);
        }
    } else {
        prev_edge = prev;
    }
}

void
version_graph::Graph::Node::set_next(std::shared_ptr<Edge> const & next, std::shared_ptr<Node> const & shared_this) {
    if (next_edge) {
        if (!(next_count++)) {
            graph.split_nodes.emplace(shared_this);
        }
    } else {
        next_edge = next;
    }
}

version_graph::Graph::Edge::Edge(const std::shared_ptr<Node> &prev, const std::shared_ptr<Node> &next, json && payload)
: prev(prev), next(next), payload(std::move(payload)) {

    DEBUG_INFO(std::string("Adding new edge (").append(prev->tag).append(", ").append(next->tag).append(", E:").append(next->tag).append(")"));
}

version_graph::Graph::iterator::iterator(version_graph::Graph &graph, std::shared_ptr<Node> current_node)
: graph(graph), current_node(std::move(current_node)) {
    if (this->current_node) {
        this->current_lock = std::shared_lock(this->current_node->next_mutex);
        this->next_node = this->current_node->next_edge ? this->current_node->next_edge->next.lock() : nullptr;
    }
}

version_graph::Graph::iterator &version_graph::Graph::iterator::operator++() {
    if (current_node) {
        graph.dispose_node(std::move(current_node));
        current_node = std::move(next_node);
        current_lock.unlock();
        if (current_node) {
            current_lock = std::shared_lock(current_node->next_mutex);
            next_node = current_node->next_edge ? current_node->next_edge->next.lock() : nullptr;
        }
    }
    return *this;
}

bool version_graph::Graph::iterator::operator!=(const version_graph::Graph::iterator &other) {
    return this->current_node != other.current_node;
}

version_graph::Graph::iterator::value_type version_graph::Graph::iterator::operator*() {
    if (!current_node) {
        throw std::runtime_error("How is current node null? ");
    }
    if (!current_node->next_edge) {
        // std::cout << "How is current node has null next edge? " << current_node->tag << std::endl;
        throw std::runtime_error("How is current node has null next edge? ");
    }
    return tie_pair<std::string const, json const>(
//            current_node->next_edge->next.lock()->tag,
            next_node->tag,
            current_node->next_edge->payload
            );
}

version_graph::Graph::iterator::iterator(version_graph::Graph &graph, std::shared_ptr<Node> current_node, version_graph::Graph::iterator::last_t)
: graph(graph), current_node(std::move(current_node)) {
}

version_graph::Graph::iterator::~iterator() {
    if (current_node)
        graph.dispose_node(std::move(current_node));
    if (next_node)
        graph.dispose_node(std::move(next_node));
}

version_graph::Graph::iterator::operator bool() const {
    return current_node && current_node->next_edge;
}

version_graph::Graph::reverse_iterator::reverse_iterator(version_graph::Graph & graph, std::shared_ptr<Node> current_node)
: graph(graph), current_node(std::move(current_node)){
    if (this->current_node) {
        this->current_lock = std::shared_lock(this->current_node->prev_mutex);
        this->next_node = this->current_node->prev_edge ? this->current_node->prev_edge->prev.lock() : nullptr;
    }
}

version_graph::Graph::reverse_iterator::reverse_iterator(
        version_graph::Graph & graph,
        std::shared_ptr<Node> current_node,
        version_graph::Graph::reverse_iterator::last_t)
        : graph(graph), current_node(std::move(current_node)){

}

version_graph::Graph::reverse_iterator & version_graph::Graph::reverse_iterator::operator++() {
    if (current_node) {
        graph.dispose_node(std::move(current_node));
        current_node = std::move(next_node);
        current_lock.unlock();
        if (current_node) {
            current_lock = std::shared_lock(current_node->prev_mutex);
            next_node = current_node->prev_edge ? current_node->prev_edge->prev.lock() : nullptr;
        }
    }
    return *this;
}

bool version_graph::Graph::reverse_iterator::operator!=(const version_graph::Graph::reverse_iterator & other) {
    return this->current_node != other.current_node;
}

version_graph::Graph::reverse_iterator::value_type version_graph::Graph::reverse_iterator::operator*() {
    return tie_pair<std::string const, json const>(
//            current_node->prev_edge->prev.lock()->tag,
            next_node->tag,
            current_node->prev_edge->payload
            );
}

version_graph::Graph::reverse_iterator::~reverse_iterator() {
    if (current_node)
        graph.dispose_node(std::move(current_node));
    if (next_node)
        graph.dispose_node(std::move(next_node));
}

version_graph::Graph::reverse_iterator::operator bool() const {
    return current_node && current_node->prev_edge;
}

std::string version_graph::Graph::get_head_tag() {
    std::shared_lock head_lock(head_mutex);
    return head->tag;
}

void version_graph::Graph::update_app_ref(const std::string &appname, const std::string &version) {
    std::shared_ptr<Node> version_node;
    {
        std::lock_guard external_refs_await_lock(external_refs_await_mutex);
        auto it = external_refs_await.extract(appname);
        if (!it.empty()) {
            if (it.mapped()->tag != version)
                dispose_node(std::move(it.mapped()));
            else
                version_node = std::move(it.mapped());
        }
    }
    if (!version_node) {
        std::shared_lock tag_to_node_lock(tag_to_node_mutex);
        auto it = tag_to_node.find(version);
        if (it != tag_to_node.end())
            version_node = it->second.lock();
    }
    if (version_node) {
        std::lock_guard external_refs_lock(external_refs_mutex);
        auto it = external_refs.find(appname);
        if (it != external_refs.end()) {
            it->second.swap(version_node);
        } else {
            external_refs.emplace(appname, std::move(version_node));
        }
    }

    if (version_node)
        dispose_node(std::move(version_node));
}

void version_graph::Graph::mark_app_ref_await(std::string const & appname, std::string const & version) {
    std::shared_ptr<Node> version_node;
    {
        std::shared_lock tag_to_node_lock(tag_to_node_mutex);
        auto it = tag_to_node.find(version);
        if (it != tag_to_node.end())
            version_node = it->second.lock();
    }

    if (version_node) {
        std::lock_guard external_refs_await_lock(external_refs_await_mutex);
        auto it = external_refs_await.find(appname);
        if (it == external_refs_await.end()) {
            external_refs_await.emplace(appname, std::move(version_node));
        } else {
            it->second.swap(version_node);
        }
    }

    if (version_node)
        dispose_node(std::move(version_node));
}
