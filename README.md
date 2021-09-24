

Step 1. Training the Model

```
$ ./train.sh ur_default
```

Step 2. Building the Index

```
$ ./bin/sqlcco_index_build --sqlite sqlcco.db --fts sqlcco.json.gz
```

Step 3. Deploying the Model

```
$ ./deploy.sh
```

Step 4. Querying the Model

```
$ ./bin/sqlcco_recommend --sqlite sqlcco-deploy.db --fts sqlcco.json.gz --entity u1
```

Step 5. Serving the Model

```
$ ./bin/sqlcco_serve --sqlite sqlcco-deploy.db --fts sqlcco.json.gz --address 0.0.0.0 --port 8080
```

Step 6. Querying the Model (via HTTP)

```
$ curl -s 'http://127.0.0.1:8080/recommendation/u1?limit=20&normalized=true' | jq
```

Update Entity History

New user browses electronics

```
$ curl -s 'http://127.0.0.1:8080/log/u5?indicator=category-browse&target=electronics'
```

New user sees phones

```
$ curl -s 'http://127.0.0.1:8080/log/u5?indicator=category-browse&target=phones'
```

New user views iphone

```
$ curl -s 'http://127.0.0.1:8080/log/u5?indicator=view&target=iphone'
```

