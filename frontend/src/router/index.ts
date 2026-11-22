import { createRouter, createWebHistory } from 'vue-router'

import CoverageView from '@/views/CoverageView.vue'
import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/backfill-coverage', name: 'backfill-coverage', component: CoverageView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
