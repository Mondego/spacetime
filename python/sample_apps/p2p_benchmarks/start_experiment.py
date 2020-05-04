import producer
import consumer
from multiprocessing import Process, Queue

def data_writer(benchmark_q):
    # Write data to disk
    while True:
        print(benchmark_q.get())

def launch_experiment(producer_count, consumer_count, experiment_length):
    # This queue will recieve performance logs from both producer/consumer
    benchmark_q = Queue()

    # start producer
    producer.main(8000, benchmark_q)
    # start consumer
    consumer.main(5000, benchmark_q)

    dw_process = Process(target=data_writer, args=(benchmark_q,))
    dw_process.start()
    dw_process.join()


def main():
    producer_count = 1
    consumer_count = 1
    experiment_length = '1h'

    launch_experiment(producer_count, consumer_count, experiment_length)

if __name__ == "__main__":
    main()