# Artifacts
This packages shows how on the basis of the hana ml python api generated sql trace artifacts can be created.

It provides the toplevel methods to generate the following target artifacts:

* HANA HDI Container artifacts
* DataHub / SAP Data Intelligence Graphs
* Abap AMDP. This is experimental.

## Installation steps:

1. Clone the repo
2. Run python setup.py install

## Getting Started:
To get started an example is provided in the example folder to provide an insight in how the artifact logic can be used. 
For more techincal information have a look at the documentation in the docu folder. 

## Additional information:
The functionality supports the following concepts

### Layered Generation (base and consumption layer)

The artifact package generation has the concept of a 2 layered approach:
1. The low level layer, aka base layer, which is the wrapper around the PAL function
and holds the defined parameters that go with the fucntion call. Input (data/model)
and output is part of the interface of the base layer procedures. The artifacts
of this layer is always HANA procedures.
2. The top level layer, aka consumption layer, which consumes the base layer procedures
and provides the correct input and uses the output. The consumption layer can be different
artifacts. For example a DataHub graph can act as consumption layer by using a python
operator to call the base layer procedures in HANA. The consumption layer can also be a
seperate HANA procudure consuming the base layer procedures.

In essence the base layer procedures and related HDI container artifacts are always
generated and depending on the users use case the respective consumption layer will
be generated. Currently 3 consumption layer targets are supported:

* HANA
* DataHub
* AMDP (ABAP) Experimental

### Data source mapping

Another concept which is provided is the notion of data source mapping. This allows
for remapping the data source. This can be helpfull in case you want to generate based on
1 experimentation with HANA ML multiple hdi containers for different target HANA systems where
the source data is stored in different tables. Please keep in mind that this functionality
is assuming the same datatype structure in the both systems.
