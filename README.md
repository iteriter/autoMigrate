## Automatic discovery and migration from MongoDB to SQL

At some point I had to migrate a MongoDB database with 30M+ records to Postgres,
including creating schema for the tables.

It would be quite convenient to have a script automatically
discover schema in Mongo, validate data, and convert it to SQL table definitions,
hence this repository.

todo:
- [x] basic schema discovery
- [ ] smart checks for sub-objects (e.g. do they have a primary key / unique keys?)
- [ ] output discovered table statistics
- [ ] discover foreign key relationships
- [ ] multi-collection discovery
- [ ] migration SQL script
- [ ] interactive command-line interface