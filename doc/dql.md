# Dokumentacja języka DQL (Data Query Language)

## Wstęp
DQL (Data Query Language) to język zapytania używany do filtrowania i agregowania danych w systemie Signomix. Język ten jest prosty, tekstowy i oparty na parametrach kluczowych.

## Podstawowa struktura zapytania
Zapytanie DQL składa się z sekwencji parametrów rozdzielonych spacjami. **Każde zapytanie musi zawierać:**
1. **Parametr `report`** - na początku zapytania
2. **Jeden z parametrów:** `eui` **lub** `group` - do identyfikacji źródła danych

Pozostałe parametry (w tym `channel`) mogą być w dowolnej kolejności.

## Parametry zapytania

### Podstawowe parametry

| Parametr | Opis | Przykład |
|----------|------|----------|
| `limit` | Ogranicza liczbę zwracanych rekordów | `limit 10` |
| `last` | Alias dla `limit` (zastarale) | `last 5` |
| `channel` | Filtruje dane po nazwie kanału | `channel temperature` |
| `eui` | Filtruje dane po identyfikatorze urządzenia | `eui 1234567890ABCDEF` |
| `group` | Filtruje dane po grupie | `group sensors` |
| `project` | Filtruje dane po projekcie | `project smart_home` |
| `tag` | Filtruje dane po tagu (format: `nazwa:wartość`) | `tag location:room1` |

### Parametry czasowe

| Parametr | Opis | Przykład |
|----------|------|----------|
| `from` | Filtruje dane od określonego czasu (format: `yyyy-mm-dd_hh:mm:ss`) | `from 2023-01-01_00:00:00` |
| `to` | Filtruje dane do określonego czasu (format: `yyyy-mm-dd_hh:mm:ss`) | `to 2023-01-31_23:59:59` |
| `sback` | Okresla offset w sekundach od bieżącego czasu | `sback 3600` (1 godzina wstecz) |

### Parametry agregacji

| Parametr | Opis | Przykład |
|----------|------|----------|
| `average` | Oblicza średnią z ostatnich N rekordów | `average 10` |
| `minimum` | Znajduje minimum z ostatnich N rekordów | `minimum 10` |
| `maximum` | Znajduje maximum z ostatnich N rekordów | `maximum 10` |
| `sum` | Sumuje ostatnie N rekordów | `sum 10` |

### Parametry sortowania

| Parametr | Opis | Przykład |
|----------|------|----------|
| `sort` | Określa kolumnę sortowania | `sort timestamp` |
| `ascending` | Sortuje rosnąco | `ascending` |
| `descending` | Sortuje malejąco | `descending` |
| `postascending` | Wymusza sortowanie rosnące po agregacji | `postascending` |

### Parametry interwałowe

| Parametr | Opis | Przykład |
|----------|------|----------|
| `interval` | Określa interwał agregacji | `interval 5 minute` |
| `second` | Interwał sekund | `interval 30 second` |
| `minute` | Interwał minut | `interval 5 minute` |
| `hour` | Interwał godzin | `interval 1 hour` |
| `day` | Interwał dni | `interval 1 day` |
| `week` | Interwał tygodni | `interval 1 week` |
| `month` | Interwał miesięcy | `interval 1 month` |
| `quarter` | Interwał kwartałów | `interval 1 quarter` |
| `year` | Interwał lat | `interval 1 year` |
| `start` | Ustawia timestamp na początku interwału | `start` |
| `end` | Ustawia timestamp na końcu interwału | `end` |
| `first` | Zwraca tylko pierwszy rekord w interwale | `first` |
| `deltas` | Oblicza różnice między rekordami w interwale | `deltas` |

### Parametry specjalne

| Parametr | Opis | Przykład |
|----------|------|----------|
| `virtual` | Oznacza zapytanie wirtualne | `virtual` |
| `report` | **Obowiązkowy** - Określa nazwę klasy pobierającej dane z bazy danych. Należy podać pełną nazwę pakietową klasy. Jeśli podana jest nazwa bez pakietu, to przyjęty zostanie pakiet domyślny `com.signomix.reports.pre` | `report com.example.DataFetcher` lub `report MonthlySummary` (z pakietem domyślnym) |
| `timeseries` | Formatuje dane jako czasowy szereg | `timeseries` |
| `notnull` | Filtruje rekordy z nulami | `notnull` |
| `skipnull` | Pomija rekordy z nulami | `skipnull` |
| `gapfill` | Wypełnia brakujące dane | `gapfill` |
| `state` | Filtruje po stanie | `state 1.0` |
| `new` | Ustawia nową wartość | `new 42.5` |
| `mpy` | Określa kanał mnożnika | `mpy multiplier_channel` |
| `mpyeui` | Określa identyfikator urządzenia mnożnika | `mpyeui 1234567890ABCDEF` |
| `format` | Określa format wyjściowy | `format csv` |
| `zone` | Określa strefę czasową | `zone Europe/Warsaw` |

### Parametry formatowania

| Parametr | Opis | Przykład |
|----------|------|----------|
| `format` | Określa format wyjściowy (csv, html, json) | `format csv` |

## Przykłady zapytań

### Proste zapytanie (z obowiązkowym parametrem report i eui)
```
report DqlReport eui abc123 channel temperature limit 10
```

### Zapytanie z grupą
```
report DqlReport group g012 channel temperature limit 10
```

### Zapytanie z filtrem czasowym
```
report DqlReport eui abc123 channel temperature from 2023-01-01_00:00:00 to 2023-01-31_23:59:59
```

### Zapytanie z agregacją
```
report DqlReport eui abc123 channel temperature average 10
```

### Zapytanie z interwałem
```
report DqlReport eui abc123 channel temperature interval 5 minute average 10
```

### Zapytanie z wieloma kanałami
```
report DqlReport eui abc123 channel temperature,humidity limit 10
```

### Zapytanie z filtrem tagów
```
report DqlReport eui abc123 tag location:room1 channel temperature
```

### Zapytanie z identyfikatorem urządzenia (eui)
```
report DqlReport eui abc123 limit 10
```

### Zapytanie z sortowaniem
```
report DqlReport eui abc123 channel temperature sort timestamp ascending
```

## Format dat
Format dat to `yyyy-mm-dd_hh:mm:ss`. Przykłady:
- `2023-01-15_14:30:00`
- `2023-12-31_23:59:59`

## Format interwałów
Interwały określają się jako `wartość nazwa_interwału`. Przykłady:
- `5 minute`
- `1 hour`
- `30 second`
- `1 day`

## Format tagów
Tagi mają format `nazwa:wartość`. Przykłady:
- `location:room1`
- `device:typeA`

## Format kanałów
Kanały można określać jako pojedynczą nazwę lub listę rozdzieloną przecinkami. Przykłady:
- `temperature`
- `temperature,humidity,pressure`

## Format wyjściowy
Domyślnie format wyjściowy to JSON. Można go zmienić za pomocą parametru `format`:
- `format csv` - format CSV
- `format html` - format HTML
- `format json` - format JSON (domyślny)

## Specjalne wartości
- `last` - alias dla `limit` (zastarale)
- `*` lub `0` w parametrze `limit` - oznacza brak limitu (wszystkie rekordy)

## Uwagi
1. Parametry można umieszczać w dowolnej kolejności
2. Jeśli nie określono `from` ani `to`, domyślnie zwracany jest ostatni rekord
3. Jeśli określono `from` lub `to`, domyślnie zwracane są wszystkie rekordy w tym zakresie
4. Parametry agregacji (`average`, `minimum`, `maximum`, `sum`) automatycznie ustawiają limit na wartość agregacji
5. Parametr `virtual` resetuje inne parametry czasowe i grupowe
