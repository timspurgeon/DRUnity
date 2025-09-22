using System;
using System.Collections.Generic;
using System.Text;
using Server.Common;

namespace Server.Game
{
    // Based on your Go objects package
    public class GCObject
    {
        public uint ID { get; set; }
        public string Name { get; set; } = "";
        public string GCType { get; set; } = "";
        public string GCLabel { get; set; } = "";
        public List<GCObjectProperty> Properties { get; set; } = new();
        public List<GCObject> Children { get; set; } = new();

        public void AddChild(GCObject child)
        {
            Children.Add(child);
        }

        // This is the key method from your Go implementation
        public void WriteFullGCObject(LEWriter writer)
        {
            // Write object header
            writer.WriteUInt32(ID);

            // Write GCType (null-terminated)
            var typeBytes = Encoding.UTF8.GetBytes(GCType);
            writer.WriteBytes(typeBytes);
            writer.WriteByte(0);

            // Write Name (null-terminated) 
            var nameBytes = Encoding.UTF8.GetBytes(Name);
            writer.WriteBytes(nameBytes);
            writer.WriteByte(0);

            // Write properties count
            writer.WriteByte((byte)Properties.Count);

            // Write each property
            foreach (var prop in Properties)
            {
                prop.Write(writer);
            }

            // Write children count
            writer.WriteByte((byte)Children.Count);

            // Write each child
            foreach (var child in Children)
            {
                child.WriteFullGCObject(writer);
            }
        }
    }

    public abstract class GCObjectProperty
    {
        public string Name { get; set; } = "";
        public abstract void Write(LEWriter writer);
    }

    public class StringProperty : GCObjectProperty
    {
        public string Value { get; set; } = "";

        public override void Write(LEWriter writer)
        {
            var nameBytes = Encoding.UTF8.GetBytes(Name);
            writer.WriteBytes(nameBytes);
            writer.WriteByte(0);

            writer.WriteByte(1); // String type

            var valueBytes = Encoding.UTF8.GetBytes(Value);
            writer.WriteBytes(valueBytes);
            writer.WriteByte(0);
        }
    }

    public class UInt32Property : GCObjectProperty
    {
        public uint Value { get; set; }

        public override void Write(LEWriter writer)
        {
            var nameBytes = Encoding.UTF8.GetBytes(Name);
            writer.WriteBytes(nameBytes);
            writer.WriteByte(0);

            writer.WriteByte(2); // UInt32 type
            writer.WriteUInt32(Value);
        }
    }

    // Factory methods based on your Go code
    public static class Objects
    {
        private static uint nextId = 1;

        public static uint NewID() => nextId++;

        public static GCObject NewPlayer(string name)
        {
            return new GCObject
            {
                ID = NewID(),
                GCType = "Player",
                Name = name,
                GCLabel = name,
                Properties = new List<GCObjectProperty>
                {
                    new StringProperty { Name = "Name", Value = name },
                    new UInt32Property { Name = "Level", Value = 1 },
                    new UInt32Property { Name = "Experience", Value = 0 },
                    new UInt32Property { Name = "Health", Value = 100 },
                    new UInt32Property { Name = "MaxHealth", Value = 100 },
                    new UInt32Property { Name = "Mana", Value = 50 },
                    new UInt32Property { Name = "MaxMana", Value = 50 },
                }
            };
        }

        public static GCObject LoadAvatar()
        {
            return new GCObject
            {
                ID = NewID(),
                GCType = "Avatar",
                Name = "PlayerAvatar",
                Properties = new List<GCObjectProperty>
                {
                    new UInt32Property { Name = "ModelID", Value = 1 },
                    new UInt32Property { Name = "Face", Value = 1 },
                    new UInt32Property { Name = "Hair", Value = 1 },
                    new UInt32Property { Name = "HairColor", Value = 1 },
                }
            };
        }

        public static GCObject NewHero(string name)
        {
            return new GCObject
            {
                ID = NewID(),
                GCType = "Hero",
                Name = name + "Hero",
                Properties = new List<GCObjectProperty>
                {
                    new UInt32Property { Name = "Level", Value = 5 },
                    new UInt32Property { Name = "Experience", Value = 1000 },
                }
            };
        }
    }
}