var AWS = require('aws-sdk'),
    _ = require('underscore');

module.exports = (function() {

    var getMatchIndices = require('waterline-criteria');

    //Tell me what environment variables exist
    console.log("AWS environment variables available:")
    _.each(process.env, function (item, key) {
        if (key.indexOf('AWS') > -1) {
            console.log("\t", key, '\t', item);
        }
    })

    //For some reason, AWS is not detecting my env variables.
    AWS.config.update({accessKeyId:process.env.ORIGINAL_EMAILS_AWS_ACCESS_KEY, secretAccessKey:process.env.ORIGINAL_EMAILS_AWS_SECRET_ACCESS_KEY});
    AWS.config.update({region:process.env.ORIGINAL_EMAILS_AWS_REGION});

    var s3 = new AWS.S3();

    function createPrefix (config, collectionName) {
        return (config.prefix + collectionName).toLowerCase();
    }

    function createKey (config, collectionName, id) {
        return createPrefix(config, collectionName) + "/" + id.toLowerCase();
    }

    function removePrefix (str, config, collectionName) {
        return str.replace(createPrefix(config, collectionName) + "/", '');
    }

    /**
     * @param {{}} where
     * @param {{}} params
     * @param {string} name
     */
    function ifExistsThenPut (where, params, name) {
        if (where[name]) { params[name] = where[name]; }
    }

    /**
     * Gernric find function
     * @param options
     */
    function find (collection, options, cb) {
        if (!collection.config.bucketName) {
            return cb(new Error("Missing bucketName in config"));
        }

        var params = { Bucket: collection.config.bucketName };
        if (options.limit === 1) {
            findOne(params, collection, options, cb);
        } else {
            findMany(params, collection, options, cb);
        }
    }

    /**
     * @param {{}} params
     * @param {string} collectionName
     * @param {{where: {}}} options
     * @param {function} cb
     */
    function findOne (params, collection, options, cb) {
        if (!options || !options.where || !options.where.id) {
            cb(new Error("Missing id"));
        } else {
            params.Key = createKey(collection.config, collection.identity, options.where.id);
            ifExistsThenPut(options.where, params, 'IfMatch');
            ifExistsThenPut(options.where, params, 'IfModifiedSince');
            ifExistsThenPut(options.where, params, 'iINoneMatch');
            s3.getObject(params, function (err, data) {
                if (data) {
                    data.id = options.where.id;
                    data.Key = createKey(collection.config, collection.identity, options.where.id);
                }
                cb(err, [data]);
            });
        }
    }

    /**
     * @param {{}} params
     * @param {string} collectionName
     * @param {{}} options
     * @param {function} cb
     */
    function findMany (params, collection, options, cb) {
        if (options.where && options.where.id && options.where.id.startsWith) {
            //the only thing we can do to limit the database request
            params.Prefix += "/" + createKey(options.where.id.startsWith);
        }
        if (options.limit) {
            params.MaxKeys = options.limit;
        }
        s3.listObjects(params, function (err, data) {
            var contents;

            if (data && data.Contents) {
                contents = data.Contents;
                //filter out unwanted keys with waterline criteria
                _.each(contents, function (item) {
                    item.id = removePrefix(item.Key, collection.config, collection.identity);
                });

                var matchIndices = getMatchIndices(contents, options);
                contents = _.select(contents, function (model, i) {
                    return _.contains(matchIndices, i);
                });
            }
            cb(err, contents);
        });
    }

    /**
     * First find the items (fetch and match criteria), then destroy
     * @param {string} collectionName
     * @param {{}} options
     * @param {function} cb
     */
    function destroy (collection, options, cb) {

        console.log("delete", options);
        var params = { Bucket: collection.config.bucketName };

        if (options.where.id) {
            destroyOne(params, collection, options, options.where.id, cb);
        } else {
            find(collection, options, function (err, data) {
                if (err || !data.length) {
                    cb(err, data);
                } else {
                    if (data.length === 1) {
                        destroyOne(params, collection, options, data[0].id, cb);
                    } else {
                        destroyMany(params, collection, options, data, cb);
                    }
                }
            });
        }
    }

    /**
     *
     * @param {{}} params
     * @param {{}} options
     * @param {[{}]} data
     * @param {function} cb
     */
    function destroyOne (params, collection, options, id, cb) {
        params.Key = createKey(collection.config, collection.identity, id);
        s3.deleteObject(params, function (err, data) {
            console.log("destroyOne", params, options, err, data);
            cb(err, data);
        });
    }

    /**
     *
     * @param {{}} params
     * @param {{}} options
     * @param {[{}]} data
     * @param {function} cb
     */
    function destroyMany (params, collection, options, data, cb) {
        params.Delete = { Objects: [] };

        _.each(data, function (item) {
            params.Delete.Objects.push({Key: item.Key});
        });

        s3.deleteObjects(params, function (err, data) {
            console.log("destroyMany", params, options, err, data);
            cb(err, data);
        });
    }

    var adapter = {
        syncable: true, // to track schema internally
        collections: {},

        defaults: {
            prefix: '',
            //private|public-read|public-read-write|authenticated-read|
            // bucket-owner-read|bucket-owner-full-control
            ACL: "bucket-owner-full-control",
            serverSideEncryption: "AES256",
            //STANDARD|REDUCED_REDUNDANCY
            storageClass: "REDUCED_REDUNDANCY",
            schema: false,
            nativeParser: false,
            safe: true
        },

        /**
         *  It's s3.  This isn't needed.
         */
        registerCollection: function(collection, cb) {
            this.collections[collection.identity] = collection;
            return cb();
        },
        teardown: function(cb) { cb(); },
        define: function(collectionName, definition, cb) { cb(); },
        describe: function(collectionName, cb) { cb(null, {}); },
        drop: function(collectionName, relations, cb) { cb(); },

        /**
         *
         * @param  {string} collectionName
         * @param  {{}} options
         * @param  {Function} cb callback
         */
        find: function(collectionName, options, cb) {
            find(this.collections[collectionName], options, cb);
        },

        /**
         *
         * REQUIRED method if users expect to call Model.destroy()
         *
         * @param  {[type]}   collectionName [description]
         * @param  {[type]}   options        [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        destroy: function(collectionName, options, cb) {
            destroy(this.collections[collectionName], options, cb);
        },

        /**
         *
         * REQUIRED method if users expect to call Model.create() or any methods
         *
         * @param  {string}   collectionName
         * @param  {{}}   values
         * @param  {Function} cb callback
         */
        create: function(collectionName, values, cb) {
            //New id that can't be guessed.  Really doesn't matter.
            values.id = Math.floor((Math.random() * 36 * 11)).toString(36); //usually ten characters
            var collection = this.collections[collectionName];

            // base64 to binary, for compression, filters, resizing, or whatever.
            var params = {
                    Bucket: this.config.bucketName,
                    Key: createKey(this.config, collectionName, values.id),
                    Body: values.Body,
                    ContentType: 'application/octet-stream',
                    //ServerSideEncryption: this.config.serverSideEncryption,
                    StorageClass: values.storageClass || this.config.storageClass
                };

            ifExistsThenPut(this.config, params, 'ACL');
            ifExistsThenPut(values, params, 'Metadata');
            ifExistsThenPut(values, params, 'Expires');
            ifExistsThenPut(values, params, 'ContentMD5');
            ifExistsThenPut(values, params, 'ContentLength');

            // Create a single new model (specified by `values`)
            s3.putObject(params, function (err, data) {

                if (data) {
                    data.id = values.id;
                }
                cb(err, data);
            });
        },

        /**
         *
         *
         * REQUIRED method if users expect to call Model.update()
         *
         * @param  {[type]}   collectionName [description]
         * @param  {[type]}   options        [description]
         * @param  {[type]}   values         [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        update: function(collectionName, options, values, cb) {
            cb(new Error("Not supported yet."));
        }
    };

    return adapter;

})();
