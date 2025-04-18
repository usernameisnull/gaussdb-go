# Description

This is a sample chat program implemented using GaussDB's listen/notify
functionality with gaussdbgo.

Start multiple instances of this program connected to the same database to chat
between them.

## Connection configuration

The database connection is configured via DATABASE_URL and standard GaussDB environment variables (PGHOST, PGUSER, etc.)

You can either export them then run chat:

    export PGHOST=/private/tmp
    ./chat

Or you can prefix the chat execution with the environment variables:

    PGHOST=/private/tmp ./chat
