package com.signomix.common;

import java.sql.Date;
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
            int zonePosition = input.indexOf("-", 1);
            char unitSymbol;
            long millis;
            String zoneId = "";
            if(zonePosition>0){
                zoneId = input.substring(zonePosition+1);
                timeString = input.substring(0,zonePosition);
                System.out.println("zoneId="+zoneId);
                System.out.println("timeString="+timeString);
            }
            millis = Long.parseLong(timeString.substring(1, timeString.length() - 1));
            unitSymbol = timeString.charAt(timeString.length() - 1);
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
                if(zoneId.isEmpty()){
                    LOG.error("Empty zone ID: " + input);
                    throw new Exception("Empty zone ID: " + input);
                }
                ts = new Timestamp(getStartOfDaysBackAsUTC(millis, zoneId));
                return ts;
            } else if (millis == 0 && multiplicand == MONTH_MILLIS) {
                // -0M means start of current month
                if(zoneId.isEmpty()){
                    LOG.error("Empty zone ID: " + input);
                    throw new Exception("Empty zone ID: " + input);
                }
                ts = new Timestamp(getStartOfMonthAsUTC(zoneId));
                return ts;
            } else if (millis != 0 && multiplicand == MONTH_MILLIS) {
                // cannot be parsed (parsing error)
                // X months back is not supported
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
                LOG.debug("(1)"+e.getMessage());
            }
            try {
                return getTimestamp(timeString, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            } catch (Exception e) {
                LOG.debug("(2)"+e.getMessage());
            }
            try {
                return getTimestamp(timeString, "yyyy-MM-dd'T'HH:mm:ssX");
            } catch (Exception e) {
                LOG.debug("(3)"+e.getMessage());
            }
            try {
                return getTimestamp(timeString, "yyyy-MM-dd'T'HH:mm:ss.SSSX");
            } catch (Exception e) {
                LOG.debug("(4)"+e.getMessage());
            }
            try {
                return getTimestamp(timeString, "yyyy-MM-dd'T'HHmmssX");
            } catch (Exception e) {
                LOG.debug("(5)"+e.getMessage());
            }
            try {
                return getTimestamp(timeString, "yyyy-MM-dd'T'HHmmss.SSSX");
            } catch (Exception e) {
                LOG.debug("(6)"+e.getMessage());
            }
            try {
                return getTimestamp(timeString, CHIRPSTACK_TIME_FORMAT);
            } catch (Exception e) {
                LOG.debug("(7)"+e.getMessage());
            }
            try {
                ts = Timestamp.from(Instant.parse(secondaryInput));
                return ts;
            } catch (Exception e) {
                LOG.debug("(8)"+e.getMessage());
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

    public static String getTimestampAsIsoInstant(Timestamp timestamp) {
        DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;
        ZonedDateTime zdtInstanceAtOffset = ZonedDateTime.ofInstant(timestamp.toInstant(), ZoneOffset.UTC);
        return zdtInstanceAtOffset.format(formatter);
    }

    public static String getTimestampAsIsoInstant(long timestamp, String zoneId) {
        DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
        ZonedDateTime zdtInstanceAtOffset = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of(zoneId));
        return zdtInstanceAtOffset.format(formatter);
    }


    private static boolean isInSeconds(long timestamp) {
        // Instant instant = Instant.ofEpochMilli(timestamp);
        // return instant.toEpochMilli() != timestamp;
        return timestamp < 9999999999L;
    }
}
