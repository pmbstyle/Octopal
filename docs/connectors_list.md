# Supported Connectors

This document lists the connectors that are currently supported in Octopal.

## Google

Current supported services:
- Gmail
- Calendar
- Drive

What it can do today:
- list recent emails
- search emails with Gmail query syntax
- read a message by ID
- read a thread by ID
- count unread emails
- inspect labels
- inspect the connected mailbox profile
- list calendars
- list events
- search events
- read an event by ID
- create an event
- update an event
- delete an event
- run free/busy lookup for one or more calendars
- list recent Drive files
- search Drive files
- inspect Drive file metadata
- list files inside Drive folders
- create Drive folders
- download Drive file contents
- export Google Docs-native files
- upload new files to Drive
- update existing Drive files
- move Drive files to trash
- move files between Drive and workspace
- upload a workspace file and return its Drive link
- read and write text files without manual base64 handling

What it does not do yet:
- send email
- archive or delete email
- mark messages read or unread
- move messages between labels/folders
- download attachment contents
- attendee response management

Setup guide:
- [google_connector_setup.md](google_connector_setup.md)

CLI flow:
1. Run `octopal configure`
2. Enable any needed Google services such as `Gmail`, `Calendar`, and/or `Drive`
3. Run `octopal connector auth google`
4. Run `octopal connector status`
5. Restart Octopal if needed

## GitHub

Current supported services:
- Repositories
- Issues
- Pull requests

What it can do today:
- inspect the authenticated GitHub account
- list repositories visible to that account
- inspect repository metadata
- list repository issues
- read a single issue
- create issues
- update issues
- list issue and pull request conversation comments
- create issue and pull request conversation comments
- update issue and pull request conversation comments
- list pull requests
- read a single pull request
- list pull request reviews
- list inline review comments on pull requests
- create pull request reviews including comment/approve/request changes
- list changed files in pull requests, including patch hunks when GitHub provides them
- list commits included in pull requests
- list commit comments for specific commit SHAs
- summarize pull request merge readiness without merging

What it does not do yet:
- create or merge pull requests
- read GitHub Actions runs
- read code contents through the connector

CLI flow:
1. Run `octopal configure`
2. Enable any needed GitHub services such as `Repositories`, `Issues`, and/or `Pull Requests`
3. Run `octopal connector auth github`
4. Run `octopal connector status`
5. Restart Octopal if needed
