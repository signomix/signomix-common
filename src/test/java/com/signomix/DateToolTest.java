package com.signomix;

import static org.junit.Assert.assertTrue;

import java.sql.Timestamp;

import org.junit.Test;

import com.signomix.common.DateTool;

/**
 * Unit test for simple App.
 */
public class DateToolTest 
{
    static long ACCEPTED_DIFF = 1000; 
    @Test
    public void test0m()
    {
        try {
            long actualTime=System.currentTimeMillis();
            long expectedTime=actualTime-0;
            Timestamp tPoint0=DateTool.parseTimestamp("-0m", null, false);
            long diff=tPoint0.getTime()-actualTime;
            assertTrue("Zbyt duża różnica czasu "+diff, tPoint0.getTime()-actualTime<1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test1m()
    {
        try {
            long expectedTime=System.currentTimeMillis()-60000;
            long calculatedTime=DateTool.parseTimestamp("-1m", null, false).getTime();
            long diff=calculatedTime-expectedTime;
            assertTrue("Zbyt duża różnica czasu "+diff, Math.abs(diff)<ACCEPTED_DIFF);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test1h()
    {
        try {
            long expectedTime=System.currentTimeMillis()-3600000;
            long calculatedTime=DateTool.parseTimestamp("-60", null, false).getTime();
            long diff=calculatedTime-expectedTime;
            assertTrue("Zbyt duża różnica czasu "+diff, Math.abs(diff)<ACCEPTED_DIFF);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
