package com.github.f1xman.bigbrother.sector.type;

import com.github.f1xman.bigbrother.Persons;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SectorVisitorsTest {

    @Test
    void createsEmptySectorVisitors() {
        SectorVisitors expectedVisitors = new SectorVisitors(new ArrayList<>());

        assertThat(SectorVisitors.createEmpty(), equalTo(expectedVisitors));
    }

    @Test
    void addsNewVisitor() {
        SectorVisitors expectedVisitors = new SectorVisitors(List.of(Persons.ALICE));
        SectorVisitors sectorVisitors = new SectorVisitors(new ArrayList<>());

        SectorVisitors actualVisitors = sectorVisitors.addVisitor(Persons.ALICE);

        assertThat(actualVisitors, equalTo(expectedVisitors));
    }

    @Test
    void removesExistingVisitor() {
        SectorVisitors expectedVisitors = new SectorVisitors(new ArrayList<>());
        SectorVisitors sectorVisitors = new SectorVisitors(List.of(Persons.ALICE));

        SectorVisitors actualVisitors = sectorVisitors.removeVisitor(Persons.ALICE);

        assertThat(actualVisitors, equalTo(expectedVisitors));
    }

    @Test
    void returnsFalseIfEmpty() {
        assertFalse(SectorVisitors.createEmpty().hasVisitors());
    }

    @Test
    void returnsTrueIfNotEmpty() {
        assertTrue(new SectorVisitors(List.of(Persons.ALICE)).hasVisitors());
    }
}