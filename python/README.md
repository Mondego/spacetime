# Spacetime Framework (Python)
===========
This is the implementation of spacetime and relational types in Python.

## Requirements and Installation
Python version required: Python 3.5+

Install the package using:
`pip3 install dist/spacetime-2.0.0-py3-none-any.whl`

# Working with spacetime
=============
## Basic application
```Python
from spacetime import Node

def my_app(dataframe):
    print ("Hello Universe!")
  
if __name__ == "__main__":
    app = Node(my_app)
    app.start()
```

`Node` is the basic worker in the spacetime system. An application needs a function to execute. In this case, we provide it the `my_app` function. Node objects are very similar to the objects created by multiprocessing.Process. An application can be launched by using the `app.start()` function call. When it is executed, the main process blocks on the call until the application finishes. To start the application asynchronously, we can start the application using the call `app.start_async()` instead. The main process launches the application and continues. Since the application is run as a daemon, the application will be terminated when the main process ends. To wait for the application to end, we can use the `app.join()` call.

Every application function, takes dataframe as the first parameter. This is of the type `rtypes.dataframe.Dataframe` and will be discussed in more details down below.

## Parameters in the function
```Python
from spacetime import Node

def my_app(dataframe, secret_msg):
    print ("Hello Universe! My secret is ", secret_msg)
  
if __name__ == "__main__":
    app = Node(my_app)
    app.start_async("not for you to know.")
    app.join()
```
Parameters can be added after the default first parameter, and can be given to the application at the time of start. Key value arguments can also be passed. You only need to make sure that the parameters you pass can be pickled (like multiprocessing.Process)

## The Dataframe.
The dataframe is a container for objects of special types (called relational types or rtypes). These objects can be synchronized between several applications and can be used as a means of inter application communication.

```Python
from spacetime import Node
from rtypes import pcc_set, primarykey, dimension

@pcc_set
class Car(object):
    vin = primarykey(int)
    color = dimension(str)
    position = dimension(tuple)
    velocity = dimension(tuple)
    
    def __init__(self, vin):
        self.vin = vin
        self.number_of_tires = 4
    
    def move(self):
        self.position = tuple([pos + vel for pos, vel in zip(self.position, self.velocity)])

def create_cars(dataframe):
    dataframe.add_many(Car, [Car(i) for i in range(10)])
  
if __name__ == "__main__":
    app = Node(create_cars, Types=[Car])
    app.start()
```
There are a lot of things to unpack in the example. We define a class `Car` and declare it to be a `pcc_set`. When a type is declared as a `pcc_set` the dataframe can track objects of that type. All objects of that type registered with the dataframe are kept together in a collection. Rtypes define both the collections that are possible, and the type of the objects within the collection. There are more complex types that can be defined within rtypes, but they will be discussed later. All of those types use `pcc_set` as the base type, and build complex collections on top of the `pcc_set` collection of objects. The type `Car` is also defined with 1 primarykey: `vin` and 3 dimensions: `color`, `position`, and `velocity`. Both `primarykey` and `dimension` are properties that are tracked by the dataframe. All other attributes and properties are local, and will not be tracked (EG: `number_of_tires`).

Only one property can be defined as the `primarykey` and defines the unique identifier for each object. If no property is set as a `primarykey` then when an object is created, a random unique id is assigned. The disadvantage of not having an explicit primary key property is that these objects cannot be extracted by index from the dataframe, and can only be obtained as part of the entire collection.

Both `primarykey` and `dimension` take in a type that signifies the type of the property and this is enforced. Additionally, `primarykey` has to be either an integer, float, or a string. (Boolean values are allowed, but they are obviously not going to be extremely useful.) Lists, and custom objects are not allowed (yet).

The line `app = Node(create_cars, Types=[Car])` defines the an application whose dataframe tracks objects of type `Car`. Multiple types can be tracked.

Objects of type `Car` can be added to the dataframe using the `add_many` method call. `dataframe.add_many(Car, [Car(i) for i in range(10)])` adds 10 `Car` objects to the dataframe in `create_cars`.

```Python
from spacetime import Node
from rtypes import pcc_set, primarykey, dimension

@pcc_set
class Car(object):
    vin = primarykey(int)
    color = dimension(str)
    position = dimension(tuple)
    velocity = dimension(tuple)
    
    def __init__(self, vin):
        self.vin = vin
        self.number_of_tires = 4
    
    def move(self):
        self.position = tuple([pos + vel for pos, vel in zip(self.position, self.velocity)])

def create_cars(dataframe):
    dataframe.add_many(Car, [Car(i) for i in range(10)])
  
def traffic_sim(dataframe):
    car_creator = Node(create_cars, Producer=[Car], dataframe=dataframe)
    car_creator.start()
    for _ in range(100):
        for car in dataframe.read_all(Car):
            car.move()

if __name__ == "__main__":
    app = Node(traffic_sim, Types=[Car])
    app.start()
```
In this example, the Main process launches one instance of the `traffic_sim` Node. This application in turn creates a single instance of the `create_cars` application and launches it. By defining the instance using `car_creator = Node(create_cars, Producer=[Car], dataframe=dataframe)`, the application is creating a sub application that can only producer new objects of type `Car`, and must synchronize the objects it creates with `traffic_sim`'s dataframe (using the keyword argument `dataframe=dataframe`). `traffic_sim` waits for `car_creator` to complete, which also ensures that all cars created are synchronized into the dataframe in `traffic_sim`. The `traffic_sim` then proceeds to move each car it has for 100 iterations before completing it's task.

TO BE CONTINUED
