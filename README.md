# INSTALLATION

This function can be run via AWS Lambda. To run this locally, run the following steps:

```.bash
npm i -g lambda-local
lambda-local -l index.js -e event.js
```

To simulate a slow query on your local machine, refer to [this stackoverflow answer](https://stackoverflow.com/a/16848240)
Run a slow query on mongo like this:

```ruby
Model.where( :$where => "sleep(100) || true" ).count
```

Same command can be mapped to mongo shell

```mongo
db.getCollection('collection_name').find("sleep(100) || true")
```
