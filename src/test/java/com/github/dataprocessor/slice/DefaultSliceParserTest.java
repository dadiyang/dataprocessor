package com.github.dataprocessor.slice;

import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DefaultSliceParserTest {


    @Test
    public void parseSlices() {
        DefaultSliceParser<LocalDateTime> parser = new DefaultSliceParser<>();
        Set<Slice<LocalDateTime>> dateTimeSlices = new HashSet<>();
        LocalDateTime start = LocalDateTime.now();
        int size = 10;
        for (int i = 0; i < size; i++) {
            LocalDateTime begin = start;
            LocalDateTime end = start.plusDays(15);
            dateTimeSlices.add(new Slice<>(begin, end));
            start = end;
        }
        String string = parser.serialize(dateTimeSlices);
        System.out.println(string);
        assertNotNull(string);
        Set<Slice<LocalDateTime>> slices = parser.parseSlices(string);
        assertEquals(size, slices.size());
        for (Slice<LocalDateTime> slice : slices) {
            assertTrue(slice.getBegin().isBefore(slice.getEnd()));
        }
    }

    @Test
    public void parseDateSlices() {
        DefaultSliceParser<Date> parser = new DefaultSliceParser<>();
        Set<Slice<Date>> dateTimeSlices = new HashSet<>();
        Date start = new Date();
        int size = 10;
        for (int i = 0; i < size; i++) {
            Date begin = start;
            Date end = new Date(start.getTime() + ((i + 1) * 86400000));
            dateTimeSlices.add(new Slice<>(begin, end));
            start = end;
        }
        String string = parser.serialize(dateTimeSlices);
        System.out.println(string);
        assertNotNull(string);
        Set<Slice<Date>> slices = parser.parseSlices(string);
        assertEquals(size, slices.size());
        for (Slice<Date> slice : slices) {
            assertTrue(slice.getBegin().before(slice.getEnd()));
        }
    }

    @Test
    public void testParse() {
        DefaultSliceParser<Date> parser = new DefaultSliceParser<>();
        Date begin = new Date();
        Date end = new Date(System.currentTimeMillis() + 86_400_000);
        Slice<Date> slice = new Slice<>(begin, end);
        String json = parser.serialize(slice);
        System.out.println(json);
        Slice<Date> slice2 = parser.parse(json);
        assertEquals(slice, slice2);
        assertTrue(slice2.getBegin().before(slice2.getEnd()));
    }

    @Test
    public void serialize() {
    }

    @Test
    public void serialize1() {
    }
}