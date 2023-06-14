package com.signomix.common;

import java.sql.Timestamp;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import org.jboss.logging.Logger;

public class DateTool {

    private static int DAY_MILLIS = 86400 * 1000;
    private static int WEEK_MILLIS = DAY_MILLIS * 7;
    private static int MONTH_MILLIS = WEEK_MILLIS * 4;
    private static int HOUR_MILLIS = 3600 * 1000;
    private static int MINUTE_MILLIS = 60 * 1000;

    public static String CHIRPSTACK_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX";

    private static final Logger LOG = Logger.getLogger(DateTool.class);

    public static Timestamp parseTimestamp(String input, String secondaryInput, boolean useSystemTimeOnError)
            throws Exception {
        if (null == input || input.isEmpty()) {
            return null;
        }
        String timeString = input.replace('~', '+').replace('_', '/');
        Timestamp ts = null;
        if (input.startsWith("-")) {
            int multiplicand = 1;
            // int zonePosition = input.indexOf("-", 1);
            char unitSymbol;
            long millis;
            String zoneId = "";
            millis = Long.parseLong(input.substring(1, input.length() - 1));
            unitSymbol = input.charAt(input.length() - 1);
            switch (unitSymbol) {
                case 'M':
                    multiplicand = MONTH_MILLIS;
                    break;
                case 'w':
                case 'W':
                    multiplicand = WEEK_MILLIS;
                    break;
                case 'd':
                case 'D':
                    multiplicand = DAY_MILLIS;
                    break;
                case 'h':
                    multiplicand = HOUR_MILLIS;
                    break;
                case 'm':
                    multiplicand = MINUTE_MILLIS;
                    break;
                case 's':
                    multiplicand = 1000;
                    break;
                default: // seconds
                    LOG.error("Unparsable input: " + input);
                    throw new Exception("Unparsable input: " + input);
            }
            if (multiplicand == DAY_MILLIS) {
                // -Xd means start of the day, X days back
                ts = new Timestamp(getStartOfDaysBackAsUTC(millis, zoneId));
                return ts;
            } else if (millis == 0 && multiplicand == MONTH_MILLIS) {
                // -0M means start of current month
                ts = new Timestamp(getStartOfMonthAsUTC(zoneId));
                return ts;
            } else if (millis != 0 && multiplicand == MONTH_MILLIS) {
                // cannot be parsed (parsing error) - actual timestamp will be returned
                LOG.error("Unparsable input: " + input);
                throw new Exception("Unparsable input: " + input);
            } else {
                ts = new Timestamp(System.currentTimeMillis() - millis * multiplicand);
                return ts;
            }
        } else {
            try {
                long millis = Long.parseLong(timeString);
                if (isInSeconds(millis)) {
                    millis = millis * 1000;
                    LOG.debug("Timestamp in seconds, multiplying by 1000");
                }
                ts = new Timestamp(millis);
                return ts;
            } catch (Exception e) {
                LOG.warn(e.getMessage());
            }
            try {
                return getTimestamp(timeString, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            } catch (Exception e) {
                LOG.warn(e.getMessage());
            }
            try {
                return getTimestamp(timeString, "yyyy-MM-dd'T'HH:mm:ssX");
            } catch (Exception e) {
                LOG.warn(e.getMessage());
            }
            try {
                return getTimestamp(timeString, "yyyy-MM-dd'T'HH:mm:ss.SSSX");
            } catch (Exception e) {
                LOG.warn(e.getMessage());
            }
            try {
                return getTimestamp(timeString, "yyyy-MM-dd'T'HHmmssX");
            } catch (Exception e) {
                LOG.warn(e.getMessage());
            }
            try {
                return getTimestamp(timeString, "yyyy-MM-dd'T'HHmmss.SSSX");
            } catch (Exception e) {
                LOG.warn(e.getMessage());
            }
            try {
                return getTimestamp(timeString, CHIRPSTACK_TIME_FORMAT);
            } catch (Exception e) {
                LOG.warn(e.getMessage());
            }
            try {
                ts = Timestamp.from(Instant.parse(secondaryInput));
                return ts;
            } catch (Exception e) {
                LOG.warn(e.getMessage());
            }
        }
        if (useSystemTimeOnError) {
            LOG.warn("Using system time");
            return new Timestamp(System.currentTimeMillis());
        } else {
            LOG.warn("Unparsable timestamp");
            throw new Exception("Unparsable timestamp");
        }
    }

    private static Timestamp getTimestamp(String input, DateTimeFormatter formatter)
            throws IllegalArgumentException, DateTimeParseException, DateTimeException, NullPointerException {
        ZonedDateTime zdtInstanceAtOffset = ZonedDateTime.parse(input, formatter);
        ZonedDateTime zdtInstanceAtUTC = zdtInstanceAtOffset.withZoneSameInstant(ZoneOffset.UTC);
        return Timestamp.from(zdtInstanceAtUTC.toInstant());
    }

    private static Timestamp getTimestamp(String input, String pattern)
            throws IllegalArgumentException, DateTimeParseException, DateTimeException, NullPointerException {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        ZonedDateTime zdtInstanceAtOffset = ZonedDateTime.parse(input, formatter);
        ZonedDateTime zdtInstanceAtUTC = zdtInstanceAtOffset.withZoneSameInstant(ZoneOffset.UTC);
        return Timestamp.from(zdtInstanceAtUTC.toInstant());
    }

    public static long getStartOfDayAsUTC(String zoneId) {
        long result;
        LocalDate localDate = LocalDate.now(ZoneId.of(zoneId));
        ZonedDateTime startOfDayInEurope2 = localDate.atTime(LocalTime.MIN)
                .atZone(ZoneId.of(zoneId));
        long offset = startOfDayInEurope2.getOffset().getTotalSeconds() * 1000;
        result = Timestamp.valueOf(startOfDayInEurope2.toLocalDateTime()).getTime() - offset;
        return result;
    }

    public static long getStartOfDaysBackAsUTC(long daysBack, String zoneId) {
        long result;
        LocalDate localDate = LocalDate.now(ZoneId.of(zoneId)).minusDays(daysBack);
        ZonedDateTime startOfDayInEurope2 = localDate.atTime(LocalTime.MIN)
                .atZone(ZoneId.of(zoneId));
        long offset = startOfDayInEurope2.getOffset().getTotalSeconds() * 1000;
        result = Timestamp.valueOf(startOfDayInEurope2.toLocalDateTime()).getTime() - offset;
        return result;
    }

    public static long getStartOfMonthAsUTC(String zoneId) {
        long result;
        LocalDate localDateNow = LocalDate.now(ZoneId.of(zoneId));
        int year = localDateNow.getYear();
        int month = localDateNow.getMonthValue();
        LocalDate localDate = LocalDate.of(year, month, 1);
        ZonedDateTime startOfDayInEurope2 = localDate.atTime(LocalTime.MIN)
                .atZone(ZoneId.of(zoneId));
        long offset = startOfDayInEurope2.getOffset().getTotalSeconds() * 1000;
        result = Timestamp.valueOf(startOfDayInEurope2.toLocalDateTime()).getTime() - offset;
        return result;
    }

    private static boolean isInSeconds(long timestamp) {
        // Instant instant = Instant.ofEpochMilli(timestamp);
        // return instant.toEpochMilli() != timestamp;
        return timestamp < 9999999999L;
    }
}
