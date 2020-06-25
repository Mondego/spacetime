#pragma once

#include <mutex>
#include <shared_mutex>

#include <unordered_set>
#include <memory>
#include <functional>
#include <unordered_map>

//#include <utils/rwlock.h>
#include <utils/utils.h>
/*
namespace version_graph{
    class Edge;
    class Node;

	class Node{
	private:
		std::shared_ptr<Edge> next_edge;
		std::shared_ptr<Edge> prev_edge;
		rwlock::rwlock_write write_lock;
	public:
        std::string tag;
		//std::unordered_set<std::weak_ptr<Node>> all_prev;
		//std::unordered_set<std::weak_ptr<Node>> all_next;
		bool is_master;
		Node(std::string tag, bool is_master);
		~Node() {std::cout << "Del node" << std::endl;}

		void set_next(std::shared_ptr<Node> const & version);
		void set_prev(std::shared_ptr<Node> const & version);
	};

	class Edge{
    public:
	    std::weak_ptr<Node> from_node;
	    std::weak_ptr<Node> to_node;
	    nlohmann::json payload;
	    Edge(std::shared_ptr<Node> from_node, std::shared_ptr<Node> to_node, nlohmann::json && payload);
	};

	class Graph{
    public:
        std::shared_ptr<Node> head;
	    std::shared_ptr<Node> tail;

    public:

        std::unordered_map<std::string, std::shared_ptr<Node>> nodes;
        std::unordered_map<std::pair<std::string, std::string>, std::shared_ptr<Edge>, utils::pair_hash<std::string, std::string>> edges;

    public:
	    Graph();
	};
}*/

namespace{
    inline static const std::string empty_tag;
}

namespace version_graph {
    using utils::json;
    class Graph {
    public:
        class Node;
        class Edge;

        class Node {
        public:
            std::shared_mutex prev_mutex;
            std::shared_mutex next_mutex;
            std::shared_ptr<Edge> prev_edge;
            std::shared_ptr<Edge> next_edge;
            std::string const tag;
            int prev_count;
            int next_count;
            bool is_master;

            Graph & graph;

            Node(std::string tag, bool is_master, Graph &graph);

            ~Node();

            void set_next(std::shared_ptr<Edge> const & next, std::shared_ptr<Node> const & shared_this);

            void set_prev(std::shared_ptr<Edge> const & prev, std::shared_ptr<Node> const & shared_this);
        };

        class Edge {
        public:
            std::weak_ptr<Node> prev;
            std::weak_ptr<Node> next;
            json payload;

            Edge(std::shared_ptr<Node> const & prev, std::shared_ptr<Node> const & next, json && payload);
        };

        class iterator {
        private:
            Graph & graph;
            std::shared_ptr<Node> current_node;
            std::shared_ptr<Node> next_node;
            std::shared_lock<std::shared_mutex> current_lock;

            struct last_t {};

        public:
            static last_t last_it;

            using value_type = std::pair<std::string const &, json const &>;

            explicit iterator(Graph & graph, std::shared_ptr<Node> current_node);

            explicit iterator(Graph & graph, std::shared_ptr<Node> current_node, last_t);

            iterator& operator++();

            bool operator!=(iterator const & other);

            operator bool() const;

            value_type operator*();

            ~iterator();
        };

        class reverse_iterator {
        private:
            Graph & graph;
            std::shared_ptr<Node> current_node;
            std::shared_ptr<Node> next_node;
            std::shared_lock<std::shared_mutex> current_lock;

            struct last_t {};

        public:
            static last_t last_it;

            using value_type = std::pair<std::string const&, json const &>;

            explicit reverse_iterator(Graph & graph, std::shared_ptr<Node> current_node);

            explicit reverse_iterator(Graph & graph, std::shared_ptr<Node> current_node, last_t);

            reverse_iterator& operator++();

            bool operator!=(reverse_iterator const & other);

            operator bool() const;

            value_type operator*();

            ~reverse_iterator();
        };


    private:
        std::shared_ptr<Node> tail;
        std::shared_ptr<Node> head;
        std::shared_mutex head_mutex;

        std::unordered_set<std::shared_ptr<Node>> disposal;
        std::mutex disposal_mutex;

        std::unordered_set<std::shared_ptr<Node>> split_nodes;
        std::unordered_set<std::shared_ptr<Node>> merge_nodes;

        std::unordered_map<std::string, std::shared_ptr<Node>> external_refs;
        std::mutex external_refs_mutex;

        std::unordered_map<std::string, std::shared_ptr<Node>> external_refs_await;
        std::mutex external_refs_await_mutex;

        void dispose_node(std::shared_ptr<Node> && node_ptr);


        std::unordered_map<std::string, std::weak_ptr<Node>> tag_to_node;
        std::shared_mutex tag_to_node_mutex;

    public:

        bool is_dead;

        void continue_chain(const std::string & from_version, std::string const & to_version, json && payload, bool force_branch = false);

        Graph();

        ~Graph();


        iterator begin(std::string const & tag = empty_tag);
        iterator end(std::string const & tag = empty_tag);

        reverse_iterator rbegin(std::string const & tag = empty_tag);
        reverse_iterator rend(std::string const & tag = empty_tag);

        std::string get_head_tag();

        void update_app_ref(std::string const & appname, std::string const & version);

        void mark_app_ref_await(std::string const & appname, std::string const & version);

        void clear_disposal();

    };
}