// The same cases as ts/test/cache.test.ts, plus the ones only this version has.
//
// The boundary being defended is in every test here: the cache is offered ALONGSIDE the real
// answer, so everything it can do wrong is a matter of offering the wrong thing — another tenant's
// data, something too old, or something subtly different from what was stored.

import 'package:multipath/multipath.dart';
import 'package:test/test.dart';

/// A store that records what it was asked to do, and can be told to fail.
class _RecordingStore extends CacheStore {
  _RecordingStore([this.initial = const {}]);

  final Map<String, String> initial;
  final Map<String, String?> writes = {};
  bool cleared = false;
  bool failLoad = false;

  @override
  Future<Map<String, String>> load() async {
    if (failLoad) throw StateError('no disk');
    return initial;
  }

  @override
  Future<void> write(String key, String? encoded) async {
    writes[key] = encoded;
  }

  @override
  Future<void> clear() async => cleared = true;
}

void main() {
  group('keyFor', () {
    test('keys by method and path', () {
      expect(RequestCache.keyFor('get', '/mt/chat'), 'GET /mt/chat');
    });

    test('ignores which line the request went over', () {
      // Keying by full URL would give every line its own cache and lose most hits the moment a
      // second line existed.
      expect(
        RequestCache.keyFor('GET', 'https://a.example.com/mt/chat'),
        RequestCache.keyFor('GET', 'https://b.example.com/mt/chat'),
      );
    });

    test('keeps the query string, which changes what was asked for', () {
      expect(
        RequestCache.keyFor('GET', '/mt/chat?page_size=50'),
        isNot(RequestCache.keyFor('GET', '/mt/chat')),
      );
    });

    test('an origin with no path at all is still a key', () {
      expect(RequestCache.keyFor('GET', 'https://a.example.com'), 'GET /');
    });
  });

  group('storing', () {
    test('returns what was stored', () {
      final cache = RequestCache()..set('GET /a', {'n': 1});
      expect(cache.get<Map<String, Object?>>('GET /a'), {'n': 1});
    });

    test('returns null for something never seen', () {
      expect(RequestCache().get<Map<String, Object?>>('GET /nope'), isNull);
    });

    test('keeps the exact value, not a copy through JSON', () {
      // A cache that hands back something subtly different from what was stored causes bugs of its
      // own, and they look like bugs in whatever read it.
      final value = [1, 2, 3];
      final cache = RequestCache()..set('GET /a', value);
      expect(identical(cache.get<List<int>>('GET /a'), value), isTrue);
    });

    test('stops offering an answer that has gone stale', () {
      var now = DateTime.utc(2026);
      final cache =
          RequestCache(maxAge: const Duration(minutes: 5), now: () => now)
            ..set('GET /a', 1);
      expect(cache.get<int>('GET /a'), 1);
      now = now.add(const Duration(minutes: 6));
      expect(cache.get<int>('GET /a'), isNull);
    });

    test('drops the least recently stored when full', () {
      final cache = RequestCache(maxEntries: 2)
        ..set('GET /a', 1)
        ..set('GET /b', 2)
        ..set('GET /c', 3);
      expect(cache.get<int>('GET /a'), isNull);
      expect(cache.get<int>('GET /c'), 3);
      expect(cache.size, 2);
    });

    test('counts a re-store as recent, so a busy entry is not evicted', () {
      final cache = RequestCache(maxEntries: 2)
        ..set('GET /a', 1)
        ..set('GET /b', 2)
        ..set('GET /a', 11)
        ..set('GET /c', 3);
      expect(cache.get<int>('GET /a'), 11, reason: 'a was used most recently');
      expect(cache.get<int>('GET /b'), isNull);
    });
  });

  group('invalidation', () {
    test('forgets everything under a prefix', () {
      final cache = RequestCache()
        ..set('GET /mt/chat', 1)
        ..set('GET /mt/chat/1/messages', 2)
        ..set('GET /mt/agent', 3);
      expect(cache.invalidate('GET /mt/chat'), 2);
      expect(cache.get<int>('GET /mt/agent'), 3);
    });

    test('clears everything on request', () {
      final cache = RequestCache()..set('GET /a', 1);
      cache.clear();
      expect(cache.size, 0);
    });
  });

  group('scope', () {
    test('drops the old tenant rather than shelving it', () {
      // Entries waiting in memory to become reachable again is exactly the accident this prevents.
      final cache = RequestCache(scope: 'user-1')..set('GET /a', 1);
      cache.setScope('user-2');
      expect(cache.get<int>('GET /a'), isNull);
      expect(cache.size, 0);
    });

    test('does nothing when the scope has not actually changed', () {
      final cache = RequestCache(scope: 'user-1')..set('GET /a', 1);
      cache.setScope('user-1');
      expect(cache.get<int>('GET /a'), 1);
    });
  });

  group('persistence', () {
    test('a stored value reaches the store', () async {
      final store = _RecordingStore();
      RequestCache(scope: 'u1', store: store).set('GET /a', {'n': 1});
      await Future<void>.delayed(Duration.zero);
      expect(store.writes.keys.single, 'u1 GET /a');
      expect(store.writes.values.single, contains('"n":1'));
    });

    test('what survived a restart is readable', () async {
      final at = DateTime.utc(2026).millisecondsSinceEpoch;
      final store =
          _RecordingStore({'u1 GET /a': '{"at":$at,"value":{"n":1}}'});
      final cache = RequestCache(
        scope: 'u1',
        store: store,
        now: () => DateTime.utc(2026, 1, 1, 1),
      );
      await cache.restore();
      expect(cache.get<Map<String, Object?>>('GET /a'), {'n': 1});
    });

    test('what survived but is too old is dropped on restore, not on read',
        () async {
      // Or a restart carries a month of dead keys around for the sake of discovering they are dead.
      final at = DateTime.utc(2026).millisecondsSinceEpoch;
      final store = _RecordingStore({'u1 GET /a': '{"at":$at,"value":1}'});
      final cache = RequestCache(
        scope: 'u1',
        store: store,
        now: () => DateTime.utc(2027),
      );
      await cache.restore();
      expect(cache.size, 0);
      expect(store.writes['u1 GET /a'], isNull);
    });

    test('an unreadable entry is a miss, not a reason to abandon the rest',
        () async {
      final at = DateTime.utc(2026).millisecondsSinceEpoch;
      final store = _RecordingStore({
        'u1 GET /bad': 'not json at all',
        'u1 GET /good': '{"at":$at,"value":7}',
      });
      final cache = RequestCache(
        scope: 'u1',
        store: store,
        now: () => DateTime.utc(2026, 1, 1, 1),
      );
      await cache.restore();
      expect(cache.get<int>('GET /good'), 7);
    });

    test('a store that cannot be read is a cold cache, not an error', () async {
      final store = _RecordingStore()..failLoad = true;
      final cache = RequestCache(store: store);
      await cache.restore();
      expect(cache.size, 0);
    });

    test('a value that cannot be encoded still caches in memory', () async {
      // Throwing here would turn "this cannot be persisted" into a failed request.
      final store = _RecordingStore();
      final cache = RequestCache(store: store);
      cache.set('GET /a', Object());
      expect(cache.get<Object>('GET /a'), isNotNull);
    });
  });
}
