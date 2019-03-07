from __future__ import print_function
from arctic import Arctic

from flask import Flask, jsonify, abort, request
# from flask_cache import Cache

app = Flask(__name__)
# cache = Cache(app, config={'CACHE_TYPE': 'simple'})
app.arc = Arctic('localhost')


@app.route('/')
def hello():
    return 'arctic explorer!'


@app.route('/libraries/')
# @cache.cached(timeout=60)
def libraries():
    return jsonify(
        [get_library_info(name, app.arc[name]) for name in app.arc.list_libraries()]
    )


def get_library_info(name, lib_obj):
    stats = lib_obj.stats()
    return {
        'symbols': lib_obj.list_symbols(),
        'name': name,
        'type': app.arc.get_library_type(name),
        'quota': app.arc.get_quota(name),
        'used': stats['dbstats']['storageSize'],
        'db': stats['dbstats']['db'],
        # 'versions': lib_obj.list_versions(),
        'last_used': '2019-01-01',  # TODO
    }


@app.route('/libraries/<name>', methods=['GET', 'POST'])
# @cache.cached(timeout=10)
def library_by_name(name):
    lib_exists = app.arc.library_exists(name)
    if request.method == 'GET':
        if not lib_exists:
            abort(404)
    else:
        if lib_exists:
            abort(409)
        post_data = request.get_json()
        lib_type = post_data['type'] if post_data and 'type' in post_data else 'VersionStore'
        app.arc.initialize_library(name, type=lib_type)

    lib = app.arc[name]
    return jsonify(get_library_info(name, lib))


app.run('0.0.0.0', 5321)