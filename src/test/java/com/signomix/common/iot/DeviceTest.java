package com.signomix.common.iot;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.Test;

import com.signomix.common.Tag;

/**
 * Unit tests for Device#getTag
 */
public class DeviceTest {

    @Test
    public void testGetTagWhenNoTags() {
        Device d = new Device();
        // default device has empty tags
        assertNull("Expected null when no tags present", d.getTag("any"));
    }

    @Test
    public void testGetTagFromString() {
        Device d = new Device();
        // set tags as serialized string name:value;name2:value2
        d.setTags("mode:auto;zone:1");
        assertEquals("auto", d.getTag("mode"));
        assertEquals("1", d.getTag("zone"));
        assertNull(d.getTag("missing"));
    }

    @Test
    public void testGetTagFromList() {
        Device d = new Device();
        List<Tag> tags = new ArrayList<>();
        Tag t1 = new Tag();
        t1.name = "k";
        t1.value = "v";
        tags.add(t1);
        Tag t2 = new Tag();
        t2.name = "x";
        t2.value = "y";
        tags.add(t2);
        d.setTags(tags);
        assertEquals("v", d.getTag("k"));
        assertEquals("y", d.getTag("x"));
    }

    @Test
    public void testMalformedTagIgnored() {
        Device d = new Device();
        d.setTags("badtagwithoutcolon");
        // malformed tag (no colon) should be ignored
        assertNull(d.getTag("badtagwithoutcolon"));
    }

    @Test
    public void testTagWithColonInValue() {
        Device d = new Device();
        // value contains additional colons -> parsing splits into more than 2 parts and is ignored
        d.setTags("name:val:with:col");
        assertNull(d.getTag("name"));
    }

    @Test
    public void testEmptyValue() {
        Device d = new Device();
        d.setTags("key:");
        // empty value should be parsed as empty string
        // Java String.split discards trailing empty tokens, so this will be ignored -> null
        assertNull(d.getTag("key"));
    }

    @Test
    public void testWhitespaceInValuesPreserved() {
        Device d = new Device();
        d.setTags("a: b;c: d");
        // parsing trims values, so spaces around values are removed
        assertEquals("b", d.getTag("a"));
        assertEquals("d", d.getTag("c"));
    }

    @Test
    public void testNullTagsString() {
        Device d = new Device();
        d.setTags((String) null);
        // null tags should result in no tags
        assertNull(d.getTag("any"));
    }

    @Test
    public void testDuplicateTagNamesReturnsFirst() {
        Device d = new Device();
        d.setTags("a:1;a:2");
        // getTag returns first occurrence
        assertEquals("1", d.getTag("a"));
    }

}
