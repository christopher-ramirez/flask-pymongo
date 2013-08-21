# Copyright (c) 2011, Dan Crosta
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice,
#   this list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

from pymongo import collection
from pymongo import mongo_client
from pymongo import database
from pymongo import mongo_replica_set_client
from bson.objectid import ObjectId

from flask import abort
import logging

class MongoClient(mongo_client.MongoClient):
    """Returns instances of :class:`flask_pymongo.wrappers.Database` instead
    of :class:`pymongo.database.Database` when accessed with dot notation.
    """

    def __getattr__(self, name):
        attr = super(MongoClient, self).__getattr__(name)
        if isinstance(attr, database.Database):
            return Database(self, name)
        return attr

class MongoReplicaSetClient(mongo_replica_set_client.MongoReplicaSetClient):
    """Returns instances of :class:`flask_pymongo.wrappers.Database`
    instead of :class:`pymongo.database.Database` when accessed with dot
    notation.  """

    def __getattr__(self, name):
        attr = super(MongoReplicaSetClient, self).__getattr__(name)
        if isinstance(attr, database.Database):
            return Database(self, name)
        return attr

class Database(database.Database):
    """Returns instances of :class:`flask_pymongo.wrappers.Collection`
    instead of :class:`pymongo.collection.Collection` when accessed with dot
    notation.
    """

    _tenant_field = None

    @property
    def tenant_field(self):
        return self._tenant_field
    @tenant_field.setter
    def tenant_field(self, value):
        self._tenant_field = value
    
    # def __init__(self, connection, name):
    #     self.tenant = None
    #     return super(Database, self).__init__(collection, name)

    def __getattr__(self, name):
        attr = super(Database, self).__getattr__(name)
        if isinstance(attr, collection.Collection):
            return Collection(self, name)
        return attr

class Collection(collection.Collection):
    """Custom sub-class of :class:`pymongo.collection.Collection` which
    adds Flask-specific helper methods.
    """

    def __getattr__(self, name):
        attr = super(Collection, self).__getattr__(name)
        if isinstance(attr, collection.Collection):
            db = self._Collection__database
            return Collection(db, attr.name)
        return attr

    def save(self, to_save, manipulate=True,
             safe=None, check_keys=True, **kwargs):

        if not isinstance(to_save, dict):
            raise TypeError("cannot save object of type %s" % type(to_save))

        if self.database.tenant_field:
            to_save.update(self.database.tenant_field)

        return super(Collection, self).save(to_save, manipulate, safe,
            check_keys, **kwargs)

    def insert(self, doc_or_docs, manipulate=True,
               safe=None, check_keys=True, continue_on_error=False, **kwargs):

        if self.database.tenant_field:
            if isinstance(doc_or_docs, (list, tuple)):
                updated_docs = []
                for doc in doc_or_docs:
                    doc.update(self.database.tenant_field)
                    updated_docsa.append(doc)
            else:
                doc_or_docs.update(self.database.tenant_field)

            return super(Collection, self).save(doc_or_docs, manipulate,
                safe, check_keys, continue_on_error, **kwargs)

    def update(self, spec, document, upsert=False, manipulate=False,
               safe=None, multi=False, check_keys=True, **kwargs):

        if not isinstance(spec, dict):
            raise TypeError("spec must be an instance of dict")

        if not isinstance(document, dict):
            raise TypeError("document must be an instance of dict")

        if self.database.tenant_field is not None:
            # Apply tenant field to query
            spec.update(self.database.tenant_field)

            # Apply tenant field to update document
            document.update(self.database.tenant_field)

        return super(Collection, self).update(
            spec, document, upsert, manipulate, safe, multi, check_keys, **kwargs
        )

    def remove(self, spec_or_id=None, safe=None, **kwargs):

        if self.database.tenant_field:
            if not isinstance(spec_or_id, dict):
                spec_or_id = {"_id": spec_or_id}

            spec_or_id.update(self.database.tenant_field)

        return super(Collection, self).remove(spec_or_id, safe, **kwargs)

    def find_one(self, spec_or_id=None, *args, **kwargs):
        logging.error(str(self.database.tenant_field))
        if self.database.tenant_field:
            if not isinstance(spec_or_id, dict):
                spec_or_id = {"_id": spec_or_id}

            spec_or_id.update(self.database.tenant_field)

        return super(Collection, self).find_one(spec_or_id, *args, **kwargs)


    def find(self, *args, **kwargs):
        logging.error(str(args))
        if self.database.tenant_field:
            if (args is None) or (args == ()):
                args = {}

            args.update(self.database.tenant_field)

        return super(Collection, self).find(*args, **kwargs)

    def find_and_modify(self, query={}, update=None,
                        upsert=False, sort=None, **kwargs):

        if self.database.tenant_field:
            query.update(self.database.tenant_field)

            if not isinstance(update, dict):
                update = {}

            update.update(self.database.tenant_field)

        return super(Collection, self).find_and_modify(query,
            update, upsert, sort, **kwargs)


    def find_one_or_404(self, *args, **kwargs):
        """Find and return a single document, or raise a 404 Not Found
        exception if no document matches the query spec. See
        :meth:`~pymongo.collection.Collection.find_one` for details.

        .. code-block:: python

            @app.route('/user/<username>')
            def user_profile(username):
                user = mongo.db.users.find_one_or_404({'_id': username})
                return render_template('user.html',
                    user=user)
        """
        found = self.find_one(*args, **kwargs)
        if found is None:
            abort(404)
        return found