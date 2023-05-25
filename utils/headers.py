import json
import re

from utils.enums import FA, XSS, TLS

HEADER_ABBREVIATION = {
    'content-security-policy': 'CSP',
    'x-frame-options': 'XFO',
    'strict-transport-security': 'HSTS',
    'referrer-policy': 'RP',
    'permissions-policy': 'PP',
    'cross-origin-opener-policy': 'COOP',
    'cross-origin-resource-policy': 'CORP',
    'cross-origin-embedder-policy': 'COEP'
}

INSECURE = {
    'content-security-policy': {'FA': FA.UNSAFE.value, 'XSS': XSS.UNSAFE.value, 'TLS': TLS.UNSAFE.value},
    'x-frame-options': 0,
    'strict-transport-security': (0, 0, 0),
    'referrer-policy': False,
    'permissions-policy': '',
    'cross-origin-opener-policy': False,
    'cross-origin-resource-policy': False,
    'cross-origin-embedder-policy': False
}


def normalize_headers(headers: dict):
    res = {}
    if 'content-security-policy' in headers:
        res['content-security-policy'] = normalize_csp(headers['content-security-policy'])
    if 'x-frame-options' in headers:
        res['x-frame-options'] = normalize_xfo(headers['x-frame-options'])
    if 'strict-transport-security' in headers:
        res['strict-transport-security'] = normalize_hsts(headers['strict-transport-security'])
    if 'referrer-policy' in headers:
        res['referrer-policy'] = normalize_referrer_policy(headers['referrer-policy'])
    if 'permissions-policy' in headers:
        res['permissions-policy'] = normalize_permissions_policy(headers['permissions-policy'])
    if 'cross-origin-opener-policy' in headers:
        res['cross-origin-opener-policy'] = normalize_coop(headers['cross-origin-opener-policy'])
    if 'cross-origin-resource-policy' in headers:
        res['cross-origin-resource-policy'] = normalize_corp(headers['cross-origin-resource-policy'])
    if 'cross-origin-embedder-policy' in headers:
        res['cross-origin-embedder-policy'] = normalize_coep(headers['cross-origin-embedder-policy'])
    return res


def classify_headers(headers, origin='https://dummy.de'):
    res = {}
    if 'content-security-policy' in headers:
        res['content-security-policy'] = json.dumps(parse_csp(origin, headers['content-security-policy']),
                                                    sort_keys=True)
    if 'x-frame-options' in headers:
        res['x-frame-options'] = get_level_xfo(headers['x-frame-options'])
    if 'strict-transport-security' in headers:
        res['strict-transport-security'] = get_level_hsts(headers['strict-transport-security'])
    if 'referrer-policy' in headers:
        res['referrer-policy'] = classify_referrer_policy(headers['referrer-policy'])
    if 'permissions-policy' in headers:
        res['permissions-policy'] = classify_permissions_policy(headers['permissions-policy'])
    if 'cross-origin-opener-policy' in headers:
        res['cross-origin-opener-policy'] = classify_coop(headers['cross-origin-opener-policy'])
    if 'cross-origin-resource-policy' in headers:
        res['cross-origin-resource-policy'] = classify_corp(headers['cross-origin-resource-policy'])
    if 'cross-origin-embedder-policy' in headers:
        res['cross-origin-embedder-policy'] = classify_coep(headers['cross-origin-embedder-policy'])
    return res


### Functions for XFO

# normalize XFO headers: convert to lower case, split over comma, sort and join over comma
def normalize_xfo(value):
    value = value.lower()
    toks = [x.strip() for x in value.split(',')]
    toks.sort()
    return ','.join(toks)


# we assume modern browsers not supporting ALLOW-FROM, so only SAMEORIGIN and DENY are considered
def get_level_xfo(value):
    value = normalize_xfo(value)
    if value == 'sameorigin':
        return 1
    if value == 'deny':
        return 2
    else:
        return 0


# comparison operator for XFO headers
def leq_xfo(v1, v2):
    return get_level_xfo(v1) <= get_level_xfo(v2)


### Functions for HSTS

# normalize HSTS headers: convert to lower case, split over semicolon, sort and join over semicolon
# note that only the first HSTS header is considered, according to RFC 6797
def normalize_hsts(value):
    value = value.lower()
    value = value.split(',')[0]
    toks = [x.strip() for x in value.split(';')]
    toks.sort()
    return ';'.join(toks)


# transforms an HSTS into an equivalent dictionary to simplify later checks
def parse_hsts(value):
    h = {}
    value = normalize_hsts(value)
    toks = value.split(';')

    for t in toks:
        if t == 'preload':
            h['preload'] = True
        if t == 'includesubdomains':
            h['isd'] = True
        if t.startswith('max-age='):
            try:
                h['max-age'] = int(t.split('=')[1])
            except ValueError:  # this handles malformed headers like max-age=1234 includesubheaders (missing ; )
                return None

    if 'preload' not in h:
        h['preload'] = False

    if 'isd' not in h:
        h['isd'] = False

    # lack of max-age implies that there is no HSTS protection
    if 'max-age' not in h:
        return None

    return h


def get_level_hsts(value):
    parsed = parse_hsts(value)
    if parsed is not None:
        return (get_level_hsts_age(parsed),
                int(parsed['isd']),
                int(parsed['preload'] and parsed['isd'] and get_level_hsts_age(parsed) == 3))
    else:
        return (get_level_hsts_age(parsed),
                0,
                0)


# auxiliary function for comparing HSTS classes, only used by leq_hsts
def get_level_hsts_age(value):
    if value is None:
        return 1
    if value['max-age'] <= 0:
        return 0
    if value['max-age'] < 31536000:
        return 2
    else:
        return 3


# comparison operator for HSTS headers, cf. the definition in the security lottery paper
def leq_hsts(v1, v2):
    v1 = parse_hsts(v1)
    v2 = parse_hsts(v2)

    c1 = get_level_hsts_age(v1)
    c2 = get_level_hsts_age(v2)

    if v1 is None:
        return v2 is None

    if v2 is None:
        return v1 is None or c1 == 0

    if v1['isd'] and not v2['isd']:
        return False

    if v1['preload'] and not v2['preload']:
        return False

    return c1 <= c2


### Functions for CSP

# normalize CSP headers: convert to lower case, split over comma (headers), semicolon (directives) and spaces (source expressions), sort everything and join again
# note: we sort in reversed order to simplify the safety check over CSP, because script-src comes before default-src
def normalize_csp(value):
    value = value.lower()
    csps = [c.strip() for c in value.split(',')]
    value_new = []

    for csp in csps:
        dirs = [d.strip() for d in csp.split(';')]
        dirs_new = []

        for d in dirs:
            toks = [t.strip() for t in d.split(' ')]
            toks_new = []

            if toks[0] in ['report-uri', 'report-to']:
                for _ in toks[1:]:
                    toks_new.append('URI')
            else:
                for tok in toks[1:]:
                    if tok.startswith("'nonce-"):
                        toks_new.append("'nonce-'")
                    else:
                        toks_new.append(tok)
            toks_new.sort()
            dirs_new.append(toks[0] + ' ' + ' '.join(toks_new))

        dirs_new.sort(reverse=True)
        value_new.append(';'.join(dirs_new))

    value_new.sort()
    return ','.join(value_new)


def is_unsafe_inline_active(sources: set) -> bool:
    allow_all_inline = False
    for source in sources:
        r = r"^('NONCE'|'nonce-[A-Za-z0-9+/\-_]+={0,2}'|'sha(256|384|512)-[A-Za-z0-9+/\-_]+={0,2}'|'strict-dynamic')$"
        if re.search(r, source, re.IGNORECASE):
            return False
        if re.match(r"^'unsafe-inline'$", source, re.IGNORECASE):
            allow_all_inline = True
    return allow_all_inline


# Paper Definition 3
def is_safe_csp(csp: dict) -> bool:
    unsafe_expressions = {'*', 'http:', 'http://', 'http://*', 'https:', 'https://', 'https://*', 'data:'}
    effective_source = csp['script-src'] if "script-src" in csp else csp.get('default-src', None)
    if effective_source is None or is_unsafe_inline_active(effective_source):
        return False
    if "'strict-dynamic'" not in effective_source and effective_source & unsafe_expressions:
        return False
    return True


def classify_framing(origin: str, sources: set) -> int:
    if sources == {"'none'"} or len(sources) == 0:
        return FA.NONE.value
    https_origin = origin.replace('http://', 'https://')
    domain_origin = origin.replace('http://', '').replace('https://', '')
    if len(sources) > 0 and len(sources - {"'self'", origin, https_origin, domain_origin}) == 0:
        return FA.SELF.value
    unsafe_expressions = {'*', 'http:', 'http://', 'http://*', 'https:', 'https://', 'https://*'}
    if sources.intersection(unsafe_expressions):
        return FA.UNSAFE.value
    return FA.CONSTRAINED.value


def classify_csp(origin: str, parsed_csp: dict) -> dict:
    classes = {'FA': FA.MISSING.value, 'XSS': XSS.MISSING.value, 'TLS': TLS.MISSING.value}
    if 'script-src' in parsed_csp or 'default-src' in parsed_csp:
        classes['XSS'] = XSS.UNSAFE.value
    if is_safe_csp(parsed_csp):
        classes['XSS'] = XSS.SAFE.value
    if 'upgrade-insecure-requests' in parsed_csp or 'block-all-mixed-content' in parsed_csp:
        classes['TLS'] = TLS.ENABLED.value
    if 'frame-ancestors' in parsed_csp:
        classes['FA'] = classify_framing(origin, parsed_csp['frame-ancestors'])
    return classes


# checks whether a list of source expression is safe with respect to XSS mitigation
def parse_csp(origin, raw_csp):
    if raw_csp is None:
        return {'FA': FA.UNSAFE.value, 'XSS': XSS.UNSAFE.value, 'TLS': TLS.UNSAFE.value}
    # Normalize Random Values
    policy_str = raw_csp
    nonce_regex = r"'nonce-[^']*'"
    policy_str = re.sub(nonce_regex, "'NONCE'", policy_str)
    report_regex = r"report-uri [^; ]*"
    policy_str = re.sub(report_regex, 'report-uri REPORT_URI;', policy_str)
    report_to = r"report-to [^; ]*"
    policy_str = re.sub(report_to, 'report-to REPORT_URI;', policy_str)
    # Let policy be a new policy with an empty directive set
    complete_policy = dict()
    # For each token returned by splitting list on commas
    for policy_string in policy_str.encode().lower().split(b','):
        # Let policy be a new policy with an empty directive set
        policy = dict()
        # For each token returned by strictly splitting serialized on the U+003B SEMICOLON character (;):
        tokens = policy_string.split(b';')
        for token in tokens:
            # Strip all leading and trailing ASCII whitespace from token.
            data = token.strip().split()
            # If token is an empty string, continue.
            if len(data) == 0:
                continue
            # Let directive name be the result of collecting a sequence of code points from
            # token which are not ASCII whitespace.
            while data[0] == ' ':
                data = data[1:]
                if len(data) == 0:
                    break
            # If token is an empty string, continue.
            if len(data) == 0:
                continue
            # Set directive name to be the result of running ASCII lowercase on directive name.
            directive_name = data[0]
            # If policy's directive set contains a directive whose name is directive name, continue.
            if directive_name in policy:
                continue
            # Let directive value be the result of splitting token on ASCII whitespace.
            directive_set = set()
            for d in data[1:]:
                if d.strip() != '':
                    directive_set.add(d.decode())
            # Append directive to policy’s directive set.
            policy[directive_name.decode()] = directive_set
        csp_classes = classify_csp(origin, policy)
        for use_case in csp_classes:
            if use_case in complete_policy:
                complete_policy[use_case] = max(complete_policy[use_case], csp_classes[use_case])
            else:
                complete_policy[use_case] = csp_classes[use_case]
    # Return policy dict
    return complete_policy


# Additional headers

def normalize_referrer_policy(value):
    return value.lower().strip()


def classify_referrer_policy(value):
    return normalize_referrer_policy(value) not in ('no-referrer-when-downgrade', 'unsafe-url')


def normalize_permissions_policy(value):
    value = value.lower()
    toks = [x.strip() for x in value.split(',')]
    toks.sort()
    return ','.join(toks)


def classify_permissions_policy(value):
    return normalize_permissions_policy(value)


def normalize_coop(value):
    return value.lower().strip()


def classify_coop(value):
    return normalize_coop(value) not in ('unsafe-none', '')


def normalize_corp(value):
    return value.lower().strip()


def classify_corp(value):
    return normalize_corp(value) not in ('cross-origin', '')


def normalize_coep(value):
    return value.lower().strip()


def classify_coep(value):
    return normalize_coep(value) not in ('unsafe-none', '')
