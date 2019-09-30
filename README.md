# Replicated Concurrency Control and Recovery: Distributed Database Design

In this project, a distributed database is implemented, equipped with multiversion concurrency control, deadlock detection, replication, and failure recovery. This project will simulate a simplified distributed database, taking inputs from an input file or standard input as execution operations, and behave like a real database with concurrency and recovery. The outputs of our project will be printed on screen.

## Project Contributors
- Yichang Chen
- Weiqiang Li

## Overview
The project consists of two main modules: transaction manager and data manager. Transaction manager is responsible for reading inputs from the inputs files and execute operations on it. It will also handle any waitlisted operations and coordinate with other modules to maintain the functions of the database system. Data manager is responsible for handling data consistency and actions on site. When a site is down or when the site recovers, the data manager would perform necessary operations to ensure data spawn among the sites are consistent. There are also other modules worth mentioning: lock manager will handle the read and write lock requests for a single variable, and deadlock manager will perform cycle detection and report to transaction manager when deadlock happens.

## Project Files
The project folder contains several files that are related:
- `src/` folder contains all the source code of this project. Since it is written in Java,
all source code files are ended with `.java`.
- `RepCRec.rpz` file is the packaged file using reprozip that can be run with
`reprounzip`.
- `test/` folder contains sample the test files of this project. They can be used as the
input of our program.
- `design_document.pdf` file serves as the design document.
Our source code is well documented. Every class has an information section showing the main feature of this class, together with author and update date. All methods in every class have general descriptions, inputs, returns or any side effect. Please refer to the source code for this information.

## How to Run
To run the project, first install `reprounzip` using `pip`. Using command line to locate to this folder, and then type:
```
> reprounzip directory setup RepCRec.rpz ~/repcrec
> reprounzip directory run ~/repcrec
```
This will start the project and run it on `test/input18.txt`. Actually, the project can either take a file as input or standard input. To run it on another file, simply type:
```
> reprounzip directory run ~/repcrec --cmdline java -jar RepCRec.jar <input_file_path>
```
Or, to run it with standard input, simply type:
```
> reprounzip directory run ~/repcrec --cmdline java -jar RepCRec.jar
```
Also, this project can be run under pure Java. The following commands will also run the project:
```
> cd src
> javac *.java
```
To run it on an input file, simply type:
```
> java RepCRec <input_file_path>
```
Or, to run it with standard input, simply type: 
```
> java RepCRec
```

## Project Report
The detailed project report can be found [here](design_document.pdf).
