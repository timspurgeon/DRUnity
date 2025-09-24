using System;
using System.Collections.Generic;
using System.Text;
using Server.Common;

namespace Server.Game
{
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

        public void WriteFullGCObject(LEWriter writer)
        {
            writer.WriteUInt32(ID);

            // Write GCType with null terminator
            foreach (var b in Encoding.UTF8.GetBytes(GCType))
                writer.WriteByte(b);
            writer.WriteByte(0);

            // Write Name with null terminator
            foreach (var b in Encoding.UTF8.GetBytes(Name))
                writer.WriteByte(b);
            writer.WriteByte(0);

            // Write properties count
            writer.WriteByte((byte)Properties.Count);
            foreach (var prop in Properties)
            {
                prop.Write(writer);
            }

            // Write children count
            writer.WriteByte((byte)Children.Count);
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
            // Write property name with null terminator
            foreach (var b in Encoding.UTF8.GetBytes(Name))
                writer.WriteByte(b);
            writer.WriteByte(0);

            // Write type (1 = string)
            writer.WriteByte(1);

            // Write value with null terminator
            foreach (var b in Encoding.UTF8.GetBytes(Value))
                writer.WriteByte(b);
            writer.WriteByte(0);
        }
    }

    public class UInt32Property : GCObjectProperty
    {
        public uint Value { get; set; }

        public override void Write(LEWriter writer)
        {
            // Write property name with null terminator
            foreach (var b in Encoding.UTF8.GetBytes(Name))
                writer.WriteByte(b);
            writer.WriteByte(0);

            // Write type (2 = uint32)
            writer.WriteByte(2);

            // Write value
            writer.WriteUInt32(Value);
        }
    }

    public static class Objects
    {
        private static uint nextId = 1;
        public static uint NewID() => nextId++;

        public static GCObject NewPlayer(string name)
        {
            var player = new GCObject
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

            // DON'T add avatar here - let WriteGoSendPlayer add it
            // var avatar = LoadAvatar();
            // player.AddChild(avatar);

            return player;
        }

        public static GCObject LoadAvatar()
        {
            var avatar = new GCObject
            {
                ID = NewID(),
                GCType = "avatar.classes.FighterFemale",
                GCLabel = "Avatar Name",
                Properties = new List<GCObjectProperty>
                {
                    new UInt32Property { Name = "Hair", Value = 0x01 },
                    new UInt32Property { Name = "HairColor", Value = 0x00 },
                    new UInt32Property { Name = "Face", Value = 0 },
                    new UInt32Property { Name = "FaceFeature", Value = 0 },
                    new UInt32Property { Name = "Skin", Value = 0x01 },
                    new UInt32Property { Name = "Level", Value = 50 },
                }
            };

            var modifiers = NewModifiers();
            avatar.AddChild(modifiers);

            var manipulators = NewManipulators();
            avatar.AddChild(manipulators);

            var dialogManager = NewDialogManager();
            avatar.AddChild(dialogManager);

            var questManager = NewQuestManager();
            avatar.AddChild(questManager);

            var avatarSkills = NewSkills();
            avatar.AddChild(avatarSkills);

            var avatarEquipment = NewEquipmentInventory();
            avatar.AddChild(avatarEquipment);

            var unitContainer = NewUnitContainer();
            avatar.AddChild(unitContainer);

            var unitBehaviour = NewUnitBehavior();
            avatar.AddChild(unitBehaviour);

            return avatar;
        }

        public static GCObject NewModifiers()
        {
            return new GCObject
            {
                ID = NewID(),
                GCType = "Modifiers",
                GCLabel = "Mod Name",
                Properties = new List<GCObjectProperty>
                {
                    new UInt32Property { Name = "IDGenerator", Value = 0x01 },
                }
            };
        }

        public static GCObject NewManipulators()
        {
            var manipulators = new GCObject
            {
                ID = NewID(),
                GCType = "Manipulators",
                GCLabel = "ManipulateMe"
            };

            var manipulator = new GCObject
            {
                ID = NewID(),
                GCType = "Manipulator",
                GCLabel = "Manipulator"
            };
            manipulators.AddChild(manipulator);

            return manipulators;
        }

        public static GCObject NewDialogManager()
        {
            return new GCObject
            {
                ID = NewID(),
                GCType = "DialogManager",
                GCLabel = "EllieDialogManager"
            };
        }

        public static GCObject NewQuestManager()
        {
            return new GCObject
            {
                ID = NewID(),
                GCType = "QuestManager",
                GCLabel = "EllieQuestManager"
            };
        }

        public static GCObject NewSkills()
        {
            var avatarSkills = new GCObject
            {
                ID = NewID(),
                GCType = "avatar.base.skills",
                GCLabel = "EllieSkills"
            };

            var skillsToAdd = new[]
            {
                new { Name = "skills.generic.Stomp", Level = 1, HotbarSlot = 0x64 },
                new { Name = "skills.generic.Sprint", Level = 1, HotbarSlot = 0x65 },
                new { Name = "skills.generic.Butcher", Level = 1, HotbarSlot = 0x66 },
                new { Name = "skills.generic.Blight", Level = 1, HotbarSlot = 0x67 },
                new { Name = "skills.generic.Charge", Level = 1, HotbarSlot = 0x68 },
                new { Name = "skills.generic.Cleave", Level = 1, HotbarSlot = 0x69 },
                new { Name = "skills.generic.IceBolt", Level = 1, HotbarSlot = 0x6a },
                new { Name = "skills.generic.IceShot", Level = 1, HotbarSlot = 0x6b },
                new { Name = "skills.generic.ManaShield", Level = 1, HotbarSlot = 0x6c },
                new { Name = "skills.generic.FearShot", Level = 1, HotbarSlot = 1 },
            };

            for (int i = 0; i < skillsToAdd.Length; i++)
            {
                var s = skillsToAdd[i];
                var skill = new GCObject
                {
                    ID = NewID(),
                    GCType = "ActiveSkill",
                    GCLabel = s.Name,
                    Properties = new List<GCObjectProperty>
                    {
                        new UInt32Property { Name = "Level", Value = (uint)s.Level },
                        new StringProperty { Name = "SkillName", Value = s.Name },
                        new UInt32Property { Name = "HotbarSlot", Value = (uint)s.HotbarSlot },
                    }
                };
                avatarSkills.AddChild(skill);
            }

            return avatarSkills;
        }

        public static GCObject NewEquipmentInventory()
        {
            var avatarEquipment = new GCObject
            {
                ID = NewID(),
                GCType = "avatar.base.Equipment",
                GCLabel = "EllieEquipment"
            };

            var armor = new GCObject
            {
                ID = NewID(),
                GCType = "PlateArmor3PAL.PlateArmor3-7",
                GCLabel = "PlateArmor"
            };
            avatarEquipment.AddChild(armor);

            var boots = new GCObject
            {
                ID = NewID(),
                GCType = "PlateBoots3PAL.PlateBoots3-7",
                GCLabel = "PlateBoots"
            };
            avatarEquipment.AddChild(boots);

            var helm = new GCObject
            {
                ID = NewID(),
                GCType = "PlateHelm3PAL.PlateHelm3-7",
                GCLabel = "PlateHelm"
            };
            avatarEquipment.AddChild(helm);

            var gloves = new GCObject
            {
                ID = NewID(),
                GCType = "PlateGloves3PAL.PlateGloves3-7",
                GCLabel = "PlateGloves"
            };
            avatarEquipment.AddChild(gloves);

            var shield = new GCObject
            {
                ID = NewID(),
                GCType = "CrystalMythicPAL.CrystalMythicShield1",
                GCLabel = "CrystalShield"
            };
            avatarEquipment.AddChild(shield);

            return avatarEquipment;
        }

        public static GCObject NewUnitContainer()
        {
            var unitContainer = new GCObject
            {
                ID = NewID(),
                GCType = "UnitContainer",
                GCLabel = "EllieUnitContainer"
            };

            var baseInventory = new GCObject
            {
                ID = NewID(),
                GCType = "avatar.base.Inventory",
                GCLabel = "EllieBaseInventory",
                Properties = new List<GCObjectProperty>
                {
                    new UInt32Property { Name = "Size", Value = 11 },
                }
            };
            unitContainer.AddChild(baseInventory);

            var bankInventory = new GCObject
            {
                ID = NewID(),
                GCType = "avatar.base.Bank",
                GCLabel = "EllieBankInventory",
                Properties = new List<GCObjectProperty>
                {
                    new UInt32Property { Name = "Size", Value = 12 },
                }
            };
            unitContainer.AddChild(bankInventory);

            var tradeInventory = new GCObject
            {
                ID = NewID(),
                GCType = "avatar.base.TradeInventory",
                GCLabel = "EllieTradeInventory",
                Properties = new List<GCObjectProperty>
                {
                    new UInt32Property { Name = "Size", Value = 13 },
                }
            };
            unitContainer.AddChild(tradeInventory);

            return unitContainer;
        }

        public static GCObject NewUnitBehavior()
        {
            return new GCObject
            {
                ID = NewID(),
                GCType = "avatar.base.UnitBehavior",
                GCLabel = "EllieBehaviour"
            };
        }

        // Legacy compatibility methods
        public static GCObject NewWeapon(string name, uint itemId)
        {
            return new GCObject
            {
                ID = NewID(),
                GCType = "Weapon",
                Name = name,
                Properties = new List<GCObjectProperty>
                {
                    new UInt32Property { Name = "ItemID", Value = itemId },
                    new UInt32Property { Name = "Damage", Value = 10 },
                    new UInt32Property { Name = "Durability", Value = 100 },
                    new StringProperty { Name = "Type", Value = "Sword" },
                }
            };
        }

        public static GCObject NewArmor(string name, uint itemId)
        {
            return new GCObject
            {
                ID = NewID(),
                GCType = "Armor",
                Name = name,
                Properties = new List<GCObjectProperty>
                {
                    new UInt32Property { Name = "ItemID", Value = itemId },
                    new UInt32Property { Name = "Defense", Value = 5 },
                    new UInt32Property { Name = "Durability", Value = 100 },
                    new StringProperty { Name = "Material", Value = "Leather" },
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
