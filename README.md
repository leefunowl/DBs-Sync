# SQL Databases Synchronization (on going project)

This is a program which can synchronize a target database with a source database.

## Testing

Run `python -m unittest discover -v` in the project's directory to test the program.

## Situations where this program can be useful

- Database migration
  - If your organization decided to migrate your old database (MS Access etc.) to a different engine (Oracle SQL etc.), and if your team does not have time or resource to re-write all the old native application (visual basic etc.). It's a solution for any organization to move forward with more modern DB engine without ditching the past.
- Mirror databases for backup
  - Instead of database sql dump, which could take lots of resources, this program simply synchronize the differences

## Limitation

- This program is not doing a realtime database synchronization.
- When there is a new table in the source DB, the schema of the new table needs to be added into target DB first. Then re-run this program.

## Further development

- Sync tables whose schema are different. For example, a `students` table in the source DB has a new column. This column of data will need to be sync into the target DB.
- Develop a program to sync database in real time efficiently.