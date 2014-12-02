## For students: change this path to the root of your code
CODEROOT = "/home/joel/workspace/dbfall14/codebase"

LDLIBS = -lreadline

#CC = gcc
CC = g++-4.8

CXX = $(CC)

#CPPFLAGS = -Wall -I$(CODEROOT) -O3  # maximal optimization
CPPFLAGS = -Wall -I$(CODEROOT) -std=c++11 -DDATABASE_FOLDER=\"$(CODEROOT)/cli/\" -g # with debugging info
