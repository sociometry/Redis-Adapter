using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Casbin.Adapter.Redis.Entities;
using Casbin.Adapter.Redis.Extensions;
using Casbin.Model;
using Casbin.Persist;
using Newtonsoft.Json;
using StackExchange.Redis;

namespace Casbin.Adapter.Redis
{
    public class RedisAdapter : RedisAdapter<CasbinRule>
    {
        public RedisAdapter() : base()
        {
        }
    }

    public class RedisAdapter<TCasbinRule> : IAdapter, IFilteredAdapter
        where TCasbinRule : class, ICasbinRule, new()
    {
        protected ConnectionMultiplexer Redis { get; }
        protected IDatabase Db { get; }
        protected IRedisOptions RedisOptions { get; }
        protected RedisKey Key { get; }
        
        public RedisAdapter()
        {
            IRedisOptions options = new RedisOptions();
            options.Address = "localhost";
            options.Password = null;
            Key = options.Key = "casbin_rules";
            
            RedisOptions = options;
            Redis = ConnectionMultiplexer.Connect("localhost");
            Db = Redis.GetDatabase();
        }
        
        public RedisAdapter(IRedisOptions redisOptions)
        {
            RedisOptions = redisOptions;
            Key = redisOptions.Key;
            Redis = ConnectionMultiplexer.Connect($"{redisOptions.Address},password={redisOptions.Password}");
            Db = Redis.GetDatabase();
        }

        ~RedisAdapter() => Redis.Close();

        public void ClearDb()
        {
            Db.KeyDelete(Key);
        }
        
        public List<List<string>> GetPoliciesInDb()
        {
            var redisValues = Db.SortedSetRangeByScore(Key);
            var casbinRules = redisValues.RedisValues2CasbinRules<TCasbinRule>();
            List<List<string>> rules = new List<List<string>>();
            foreach (var casbinRule in casbinRules)
            {
                List<string> line = new List<string>();
                if (casbinRule.V0 is not null)
                {
                    line.Add(casbinRule.V0);
                }
                if (casbinRule.V1 is not null)
                {
                    line.Add(casbinRule.V1);
                }
                if (casbinRule.V2 is not null)
                {
                    line.Add(casbinRule.V2);
                }
                if (casbinRule.V3 is not null)
                {
                    line.Add(casbinRule.V3);
                }
                if (casbinRule.V4 is not null)
                {
                    line.Add(casbinRule.V4);
                }
                if (casbinRule.V5 is not null)
                {
                    line.Add(casbinRule.V5);
                }
                rules.Add(line);
            }
            return rules;
        }

        #region virtual method
        protected virtual IEnumerable<TCasbinRule> OnLoadPolicy(IPolicyStore model, IEnumerable<TCasbinRule> casbinRules)
        {
            return casbinRules;
        }
        
        protected virtual IEnumerable<TCasbinRule> OnSavePolicy(IPolicyStore model, IEnumerable<TCasbinRule> casbinRules)
        {
            return casbinRules;
        }
        
        protected virtual TCasbinRule OnAddPolicy(string section, string policyType, IEnumerable<string> rule, TCasbinRule casbinRule)
        {
            return casbinRule;
        }

        protected virtual IEnumerable<TCasbinRule> OnAddPolicies(string section, string policyType,
            IEnumerable<IEnumerable<string>> rules, IEnumerable<TCasbinRule> casbinRules)
        {
            return casbinRules;
        }

        protected virtual IEnumerable<TCasbinRule> OnRemoveFilteredPolicy(string section, string policyType, 
            int fieldIndex, string[] fieldValues, IEnumerable<TCasbinRule> casbinRules)
        {
            return casbinRules;
        }
        
        #endregion
        
        #region Load policy
        
        public virtual void LoadPolicy(IPolicyStore model)
        {
            var redisValues = Db.SortedSetRangeByScore(Key);
            var casbinRules = redisValues.RedisValues2CasbinRules<TCasbinRule>();
            casbinRules = OnLoadPolicy(model, casbinRules);
            model.LoadPolicyFromCasbinRules(casbinRules);
            IsFiltered = false;
        }
        
        public virtual async Task LoadPolicyAsync(IPolicyStore model)
        {
            var redisValues = await Db.SortedSetRangeByScoreAsync(Key);
            var casbinRules = redisValues.RedisValues2CasbinRules<TCasbinRule>();
            casbinRules = OnLoadPolicy(model, casbinRules);
            model.LoadPolicyFromCasbinRules(casbinRules);
            IsFiltered = false;
        }
        
        #endregion
        
        #region Save policy
        
        public virtual void SavePolicy(IPolicyStore model)
        {
            var casbinRules = new List<TCasbinRule>();
            casbinRules.ReadPolicyFromCasbinModel(model);

            if (casbinRules.Count is 0)
            {
                return;
            }

            var saveRules = OnSavePolicy(model, casbinRules);

            Db.KeyDelete(Key);
            foreach (var saveRule in saveRules)
            {
                Db.SortedSetAdd(Key, JsonConvert.SerializeObject(saveRule), DateTime.UtcNow.Ticks);
            }
        }
        
        public virtual async Task SavePolicyAsync(IPolicyStore model)
        {
            var casbinRules = new List<TCasbinRule>();
            casbinRules.ReadPolicyFromCasbinModel(model);

            if (casbinRules.Count is 0)
            {
                return;
            }

            var saveRules = OnSavePolicy(model, casbinRules);

            await Db.KeyDeleteAsync(Key);
            foreach (var saveRule in saveRules)
            {
                await Db.SortedSetAddAsync(Key, JsonConvert.SerializeObject(saveRule), DateTime.UtcNow.Ticks);
            }
        }
        
        #endregion
        
        #region Add policy
        
        public virtual void AddPolicy(string section, string policyType, IEnumerable<string> rule)
        {
            if (rule is null || rule.Count() is 0)
            {
                return;
            }

            var casbinRule = CasbinRuleExtenstion.Parse<TCasbinRule>(policyType, rule);
            casbinRule = OnAddPolicy(section, policyType, rule, casbinRule);
            Db.SortedSetAdd(Key, JsonConvert.SerializeObject(casbinRule), DateTime.UtcNow.Ticks);
        }
        
        public virtual async Task AddPolicyAsync(string section, string policyType, IEnumerable<string> rule)
        {
            if (rule is null || rule.Count() is 0)
            {
                return;
            }

            var casbinRule = CasbinRuleExtenstion.Parse<TCasbinRule>(policyType, rule);
            casbinRule = OnAddPolicy(section, policyType, rule, casbinRule);
            await Db.SortedSetAddAsync(Key, JsonConvert.SerializeObject(casbinRule), DateTime.UtcNow.Ticks);
        }
        
        public virtual void AddPolicies(string section, string policyType, IEnumerable<IEnumerable<string>> rules)
        {
            if (rules is null)
            {
                return;
            }

            var rulesArray = rules as IList<string>[] ?? rules.ToArray();
            if (rulesArray.Length is 0)
            {
                return;
            }

            var casbinRules = rulesArray.Select(r => 
                CasbinRuleExtenstion.Parse<TCasbinRule>(policyType, r.ToList()));
            casbinRules = OnAddPolicies(section, policyType, rulesArray, casbinRules);
            foreach (var casbinRule in casbinRules)
            {
                Db.SortedSetAdd(Key, JsonConvert.SerializeObject(casbinRule), DateTime.UtcNow.Ticks);
            }
        }
        
        public virtual async Task AddPoliciesAsync(string section, string policyType, IEnumerable<IEnumerable<string>> rules)
        {
            if (rules is null)
            {
                return;
            }

            var rulesArray = rules as IList<string>[] ?? rules.ToArray();
            if (rulesArray.Length is 0)
            {
                return;
            }

            var casbinRules = rulesArray.Select(r => 
                CasbinRuleExtenstion.Parse<TCasbinRule>(policyType, r.ToList()));
            casbinRules = OnAddPolicies(section, policyType, rulesArray, casbinRules);
            foreach (var casbinRule in casbinRules)
            {
                await Db.SortedSetAddAsync(Key, JsonConvert.SerializeObject(casbinRule), DateTime.UtcNow.Ticks);
            }
        }
        
        #endregion
        
        #region Remove policy

        public virtual void RemovePolicy(string section, string policyType, IEnumerable<string> rule)
        {
            if (rule is null || rule.Count() is 0)
            {
                return;
            }

            RemoveFilteredPolicy(section, policyType, 0, rule as string[] ?? rule.ToArray());
        }

        public virtual async Task RemovePolicyAsync(string section, string policyType, IEnumerable<string> rule)
        {
            if (rule is null || rule.Count() is 0)
            {
                return;
            }

            await RemoveFilteredPolicyAsync(section, policyType, 0, rule as string[] ?? rule.ToArray());        
        }
        
        public virtual void RemoveFilteredPolicy(string section, string policyType, int fieldIndex, params string[] fieldValues)
        {
            if (fieldValues is null || fieldValues.Length is 0)
            {
                return;
            }

            var redisRules = Db.SortedSetRangeByScore(Key);
            var casbinRules = redisRules.ApplyQueryFilter<TCasbinRule>(policyType, fieldIndex, fieldValues);
            casbinRules = OnRemoveFilteredPolicy(section, policyType, fieldIndex, fieldValues, casbinRules);
            
            foreach (var casbinRule in casbinRules)
            {
                Db.SortedSetRemove(Key, JsonConvert.SerializeObject(casbinRule));
            }
        }
        
        public virtual async Task RemoveFilteredPolicyAsync(string section, string policyType, int fieldIndex, params string[] fieldValues)
        {
            if (fieldValues is null || fieldValues.Length is 0)
            {
                return;
            }

            var redisRules = await Db.SortedSetRangeByScoreAsync(Key);
            var casbinRules = redisRules.ApplyQueryFilter<TCasbinRule>(policyType, fieldIndex, fieldValues);
            casbinRules = OnRemoveFilteredPolicy(section, policyType, fieldIndex, fieldValues, casbinRules);
            
            foreach (var casbinRule in casbinRules)
            {
                await Db.SortedSetRemoveAsync(Key, JsonConvert.SerializeObject(casbinRule));
            }        
        }


        public virtual void RemovePolicies(string section, string policyType, IEnumerable<IEnumerable<string>> rules)
        {
            if (rules is null)
            {
                return;
            }

            var rulesArray = rules as IList<string>[] ?? rules.ToArray();
            if (rulesArray.Length is 0)
            {
                return;
            }

            foreach (var rule in rulesArray)
            {
                RemoveFilteredPolicy(section, policyType, 0, rule as string[] ?? rule.ToArray());
            }
        }

        public virtual async Task RemovePoliciesAsync(string section, string policyType, IEnumerable<IEnumerable<string>> rules)
        {
            if (rules is null)
            {
                return;
            }

            var rulesArray = rules as IList<string>[] ?? rules.ToArray();
            if (rulesArray.Length is 0)
            {
                return;
            }

            foreach (var rule in rulesArray)
            {
                await RemoveFilteredPolicyAsync(section, policyType, 0, rule as string[] ?? rule.ToArray());
            }
        }

        #endregion

        #region IFilteredAdapter

        public bool IsFiltered { get; private set; }

        public void LoadFilteredPolicy(IPolicyStore model, Filter filter)
        {
            var redisValues = Db.SortedSetRangeByScore(Key);
            var casbinRules = redisValues.ApplyQueryFilter<TCasbinRule>(filter);
            casbinRules = OnLoadPolicy(model, casbinRules);
            model.LoadPolicyFromCasbinRules(casbinRules);
            IsFiltered = true;
        }

        public async Task LoadFilteredPolicyAsync(IPolicyStore model, Filter filter)
        {
            var redisValues = await Db.SortedSetRangeByScoreAsync(Key);
            var casbinRules = redisValues.ApplyQueryFilter<TCasbinRule>(filter);
            casbinRules = OnLoadPolicy(model, casbinRules);
            model.LoadPolicyFromCasbinRules(casbinRules);
            IsFiltered = true;
        }

        #endregion
    }
}