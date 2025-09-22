
using System;
using System.Collections.Generic;

namespace Server.Store {
  public class MemoryStore : IStore {
    int nextAccount = 1;
    int nextChar = 1;
    readonly Dictionary<string,(int id,string pass)> accounts = new();
    readonly Dictionary<int,List<CharacterRow>> chars = new();

    public int CheckUser(string name, string pass) {
      if (accounts.TryGetValue(name, out var tup)) {
        return tup.pass == pass ? tup.id : -1;
      }
      int id = nextAccount++;
      accounts[name] = (id, pass);
      chars[id] = new List<CharacterRow>();
      return id;
    }

    public List<CharacterRow> ListCharacters(int accountId) {
      return chars.TryGetValue(accountId, out var list) ? new List<CharacterRow>(list) : new List<CharacterRow>();
    }

    public bool CreateCharacter(int accountId, string name, int classId, out string? fail) {
      fail = null;
      if (!chars.ContainsKey(accountId)) chars[accountId] = new List<CharacterRow>();
      foreach (var list in chars.Values)
        foreach (var c in list)
          if (string.Equals(c.Name, name, System.StringComparison.OrdinalIgnoreCase)) { fail = "Name taken"; return false; }
      var row = new CharacterRow { Id = nextChar++, Name = name, ClassId = classId, Level = 1 };
      chars[accountId].Add(row);
      return true;
    }
  }
}
