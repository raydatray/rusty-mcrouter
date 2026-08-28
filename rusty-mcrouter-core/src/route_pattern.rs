/// Matches mcrouter routing-prefix patterns. `*` may match bytes only within
/// one slash-delimited component and, like mcrouter, never matches an empty
/// remaining route at the end of a pattern.
pub(crate) fn matches(pattern: &[u8], route: &[u8]) -> bool {
    if pattern.is_empty() || route.is_empty() {
        return pattern.is_empty() && route.is_empty();
    }

    let mut pattern_index = 0;
    let mut route_index = 0;
    let mut star_pattern = None;
    let mut star_route = 0;

    while route_index < route.len() {
        if pattern_index < pattern.len()
            && pattern[pattern_index] != b'*'
            && pattern[pattern_index] == route[route_index]
        {
            pattern_index += 1;
            route_index += 1;
            continue;
        }

        if pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
            while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
                pattern_index += 1;
            }

            if pattern_index == pattern.len() {
                return !route[route_index..].contains(&b'/');
            }

            star_pattern = Some(pattern_index);
            star_route = route_index;
            continue;
        }

        let Some(retry_pattern) = star_pattern else {
            return false;
        };
        if star_route == route.len() || route[star_route] == b'/' {
            return false;
        }

        star_route += 1;
        route_index = star_route;
        pattern_index = retry_pattern;
    }

    pattern_index == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_upstream_route_patterns() {
        for (pattern, route) in [
            ("/a/b/", "/a/b/"),
            ("/a/*/", "/a/b/"),
            ("/a/a*c/", "/a/abc/"),
            ("/a/a*c/", "/a/abbbbbbbbbbbbbbbc/"),
            ("/a*c/d*f/", "/abbbbc/deeeeeeeeeeeef/"),
            ("/a****/d*f/", "/abbbbc/deeeeeeeeeeeef/"),
            ("/*/*/", "/aaa/bbb/"),
            ("/*baf*/", "/aaabafggg/"),
            ("/*1*2*3*4*5/", "/aaa1bbb2bbb3af4sdgfsdg5/"),
            ("*", "a"),
            ("/*a/a/", "/a/a/"),
            ("/a/*a/", "/a/a/"),
        ] {
            assert!(
                matches(pattern.as_bytes(), route.as_bytes()),
                "{pattern} {route}"
            );
        }
    }

    #[test]
    fn rejects_upstream_route_non_matches() {
        for (pattern, route) in [
            ("*", ""),
            ("*", "/"),
            ("*/*/", "a/b"),
            ("*", "a/b"),
            ("****", "/b"),
        ] {
            assert!(
                !matches(pattern.as_bytes(), route.as_bytes()),
                "{pattern} {route}"
            );
        }
    }

    #[test]
    fn matches_arbitrary_bytes_without_crossing_slashes() {
        assert!(matches(b"/a\xff*/b/", b"/a\xffx/b/"));
        assert!(!matches(b"/a*/b/", b"/a/x/b/"));
        assert!(!matches(b"a*", b"a"));
    }
}
