package br.com.mydb;

public record Column(String name, DataType type, int ordinalPosition) {}
