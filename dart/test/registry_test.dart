// The same cases as ts/test/registry.test.ts and go/registry_test.go.
//
// Deliberately so: the registry document is the one vocabulary every package shares, and a rule
// that is tightened in one parser and not the others is a disagreement that only shows up in
// production, on whichever client happens to be strictest.

import 'package:multipath/multipath.dart';
import 'package:test/test.dart';

void main() {
  group('accepts', () {
    test('a single same-origin line, which is the adoption case', () {
      final registry = parseRegistry({
        'lines': [
          {
            'id': 'origin',
            'url': '',
            'transport': 'same-origin',
            'weight': 100
          },
        ],
      });
      expect(registry.lines, hasLength(1));
      expect(registry.lines.single.id, 'origin');
      expect(registry.lines.single.url, isEmpty);
      expect(registry.lines.single.weight, 100);
    });

    test('a JSON string, so a response body can be handed over directly', () {
      final registry = parseRegistry('{"lines":[{"id":"a","url":""}]}');
      expect(registry.lines.single.id, 'a');
    });

    test('null on an optional field, treating it as absent', () {
      // This is 0.1.1's fix, and it is worth a test of its own: an ordinary serializer emits nulls
      // for unset optionals, and rejecting the document whole for one of them made a real
      // deployment believe it had two lines while quietly using one.
      final registry = parseRegistry({
        'lines': [
          {
            'id': 'a',
            'url': null,
            'transport': null,
            'weight': null,
            'foreignOrigin': null,
          },
        ],
      });
      expect(registry.lines.single.url, isEmpty);
      expect(registry.lines.single.weight, 0);
      expect(registry.lines.single.foreignOrigin, isFalse);
    });

    test('an absolute origin', () {
      final registry = parseRegistry({
        'lines': [
          {'id': 'cf', 'url': 'https://cf.example.com'},
        ],
      });
      expect(registry.lines.single.url, 'https://cf.example.com');
    });

    test('an origin carrying a port', () {
      final registry = parseRegistry({
        'lines': [
          {'id': 'local', 'url': 'http://127.0.0.1:8080'},
        ],
      });
      expect(registry.lines.single.url, 'http://127.0.0.1:8080');
    });
  });

  group('rejects', () {
    test('a document with no lines', () {
      expect(
        () => parseRegistry({'lines': <Object>[]}),
        throwsA(isA<RegistryFormatException>()),
      );
    });

    test('a line with no id', () {
      expect(
        () => parseRegistry({
          'lines': [
            {'url': ''},
          ],
        }),
        throwsA(isA<RegistryFormatException>()),
      );
    });

    test('a duplicate id, because a metric that lies is believed', () {
      expect(
        () => parseRegistry({
          'lines': [
            {'id': 'a', 'url': ''},
            {'id': 'a', 'url': 'https://b.example.com'},
          ],
        }),
        throwsA(isA<RegistryFormatException>()),
      );
    });

    test('a url with a path', () {
      expect(
        () => parseRegistry({
          'lines': [
            {'id': 'a', 'url': 'https://a.example.com/mt'},
          ],
        }),
        throwsA(isA<RegistryFormatException>()),
      );
    });

    test('a url with a trailing slash, which would produce "//mt/probe"', () {
      expect(
        () => parseRegistry({
          'lines': [
            {'id': 'a', 'url': 'https://a.example.com/'},
          ],
        }),
        throwsA(isA<RegistryFormatException>()),
      );
    });

    test('a scheme that is not http(s)', () {
      expect(
        () => parseRegistry({
          'lines': [
            {'id': 'a', 'url': 'wss://a.example.com'},
          ],
        }),
        throwsA(isA<RegistryFormatException>()),
      );
    });
  });

  group('resolve', () {
    test('a same-origin line emits exactly the path a plain client would', () {
      expect(const Line(id: 'a').resolve('/mt/chat'), '/mt/chat');
    });

    test('an absolute line prefixes its origin', () {
      const line = Line(id: 'a', url: 'https://a.example.com');
      expect(line.resolve('/mt/chat'), 'https://a.example.com/mt/chat');
    });

    test('a path that does not start with a slash is refused here', () {
      // Rather than concatenated into something that looks like a URL and is not.
      expect(() => const Line(id: 'a').resolve('mt/chat'), throwsArgumentError);
    });
  });
}
