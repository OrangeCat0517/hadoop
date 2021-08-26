package com.example.domain;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDate;

public class User implements Writable {
    private int id;
    private String name;
    private LocalDate birthday;

    public User() {
    }

    public User(int id, String name, LocalDate birthday) {
        this.id = id;
        this.name = name;
        this.birthday = birthday;
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", birthday=" + birthday +
                '}';
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(name);
        out.writeInt(birthday.getYear());
        out.writeInt(birthday.getMonth().getValue());
        out.writeInt(birthday.getDayOfMonth());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.name = in.readUTF();
        this.birthday = LocalDate.of(in.readInt(), in.readInt(), in.readInt());
    }
}
