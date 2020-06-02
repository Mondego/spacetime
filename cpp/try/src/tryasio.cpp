//
// Created by zhazha on 2/11/20.
//

#include <asio.hpp>
#include <iostream>
#include <mutex>

using asio::ip::udp;

int count;
std::mutex count_mutex;

std::string make_daytime_string()
{
    using namespace std; // For time_t, time and ctime;
    time_t now = time(0);
    return ctime(&now);
}

class udp_server
{
public:
    udp_server(asio::io_context& io_context)
            : socket_(io_context, udp::endpoint(udp::v4(), 13))
    {
        start_receive();
    }

private:
    void start_receive()
    {
        socket_.async_receive_from(
                asio::buffer(recv_buffer_), remote_endpoint_,
                /*std::bind(&udp_server::handle_receive, this,
                            std::placeholders::_1,
                            std::placeholders::_2)*/
                [&](const asio::error_code& error,
                    std::size_t bytes) { this->handle_receive(error, bytes); } );
    }

    void handle_receive(const asio::error_code& error,
                        std::size_t /*bytes_transferred*/)
    {
        if (!error)
        {
            auto message = std::make_shared<std::string>(make_daytime_string());

            {
                std::lock_guard lock(count_mutex);
                std::cout << count++ << std::endl;
            }

            socket_.async_send_to(asio::buffer(*message), remote_endpoint_,
                                  std::bind(&udp_server::handle_send, this, message,
                                              std::placeholders::_1,
                                              std::placeholders::_2));

            start_receive();
        }
    }

    void handle_send(std::shared_ptr<std::string> /*message*/,
                     const asio::error_code& /*error*/,
                     std::size_t /*bytes_transferred*/)
    {
    }

    udp::socket socket_;
    udp::endpoint remote_endpoint_;
    std::array<char, 1> recv_buffer_;
};

int main()
{
    try
    {
        asio::io_context io_context;
        udp_server server(io_context);
        io_context.run();
    }
    catch (std::exception& e)
    {
        std::cerr << e.what() << std::endl;
    }

    return 0;
}